use clap::Parser;
use futures::stream::{self, StreamExt};
use ironcrab::config::Config;
use ironcrab::solana::dex::pumpfun::PumpFunDex;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::wallet::Treasury;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::bs58;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::Transaction;
use solana_sdk::transaction::VersionedTransaction;
use solana_transaction_status::UiTransactionEncoding;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde_json::json;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, default_value = "my_config.server.toml")]
    config: PathBuf,

    /// Override RPC URL (e.g. https://api.mainnet-beta.solana.com)
    #[arg(long)]
    rpc_url: Option<String>,

    /// Force burn without confirmation (DANGEROUS - use with caution)
    #[arg(long, default_value = "false")]
    force_burn: bool,

    /// Override Jupiter base URL (default: https://quote-api.jup.ag).
    /// Useful if the default hostname has DNS issues in your environment.
    /// Example: --jupiter-base-url https://api.jup.ag
    #[arg(long)]
    jupiter_base_url: Option<String>,
}

#[derive(Debug)]
struct SellTask {
    mint: Pubkey,
    amount: u64,
    ta_pubkey: Pubkey,
}

/// Check if a token is on Pump.fun bonding curve (not migrated to Raydium yet)
fn is_pumpfun_mint(mint: &Pubkey) -> bool {
    // Pump.fun mints typically end with "pump" in base58
    let mint_str = mint.to_string();
    mint_str.ends_with("pump")
}

/// Last resort: burn all tokens and close the account to recover rent (~0.002 SOL)
async fn burn_and_close_account(
    rpc: Arc<SolanaRpc>,
    treasury: Arc<Treasury>,
    task: &SellTask,
) -> anyhow::Result<()> {
    let token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();

    info!(
        "No liquidity for {} - burning {} tokens and closing account to recover rent...",
        task.mint, task.amount
    );

    // Build burn instruction manually (SPL Token instruction index 8 = Burn)
    // Accounts: [token_account (writable), mint (writable), owner/delegate (signer)]
    // Data: [8 (instruction index), amount (u64 LE)]
    let mut burn_data = vec![8u8]; // Burn instruction index
    burn_data.extend_from_slice(&task.amount.to_le_bytes());

    let burn_ix = Instruction {
        program_id: token_program_id,
        accounts: vec![
            AccountMeta::new(task.ta_pubkey, false), // token account (writable)
            AccountMeta::new(task.mint, false),      // mint (writable)
            AccountMeta::new_readonly(treasury.pubkey(), true), // owner (signer)
        ],
        data: burn_data,
    };

    // Build close account instruction manually (SPL Token instruction index 9 = CloseAccount)
    // Accounts: [account_to_close (writable), destination (writable), owner (signer)]
    let close_ix = Instruction {
        program_id: token_program_id,
        accounts: vec![
            AccountMeta::new(task.ta_pubkey, false), // account to close (writable)
            AccountMeta::new(treasury.pubkey(), false), // destination for rent (writable)
            AccountMeta::new_readonly(treasury.pubkey(), true), // owner (signer)
        ],
        data: vec![9u8], // CloseAccount instruction index
    };

    let latest_blockhash = rpc.get_latest_blockhash_retry().await?;
    let mut tx = Transaction::new_with_payer(&[burn_ix, close_ix], Some(&treasury.pubkey()));

    if let Err(e) = tx.try_sign(&[treasury.signer_ref()], latest_blockhash) {
        warn!("Failed to sign burn+close for {}: {:?}", task.mint, e);
        return Err(anyhow::anyhow!("Failed to sign"));
    }

    match rpc.rpc.send_and_confirm_transaction(&tx).await {
        Ok(sig) => {
            info!(
                "Burned {} tokens and closed account for {}! Recovered rent. Sig: {}",
                task.amount, task.mint, sig
            );
            Ok(())
        }
        Err(e) => {
            warn!("Failed to burn+close {}: {:?}", task.mint, e);
            Err(anyhow::anyhow!("TX failed: {:?}", e))
        }
    }
}

/// Result of sell attempt - either sold or needs burn
#[derive(Debug)]
enum SellResult {
    Sold,
    NeedsConfirmation(SellTask, String), // task + reason why it failed
}

#[derive(Debug, serde::Deserialize)]
struct JupiterSwapResponse {
    #[serde(rename = "swapTransaction")]
    swap_transaction: String,
}

async fn try_sell_jupiter(
    rpc: Arc<SolanaRpc>,
    treasury: Arc<Treasury>,
    task: &SellTask,
    jupiter_base_url: &str,
) -> bool {
    // Jupiter fallback is useful for migrated Pump.fun tokens (e.g. PumpSwap liquidity)
    // and for any token where our native DEX connectors don't have coverage.
    let mint = task.mint;
    let amount = task.amount;
    let sol_mint = "So11111111111111111111111111111111111111112";
    let slippage_bps = 9900u16; // panic sell

    info!("Trying Jupiter for {} ({} tokens)...", mint, amount);

    let client = reqwest::Client::new();
    let primary_base = jupiter_base_url.trim_end_matches('/');
    let mut bases = vec![primary_base.to_string()];

    // Safe fallback: try api.jup.ag with the same v6 paths if the default quote-api host
    // is broken/unresolvable in the environment.
    if primary_base == "https://quote-api.jup.ag" {
        bases.push("https://api.jup.ag".to_string());
    }

    let mut swap_json: Option<JupiterSwapResponse> = None;

    for base in bases {
        info!("Jupiter base URL: {}", base);

        let quote_url = format!(
            "{}/v6/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}",
            base, mint, sol_mint, amount, slippage_bps
        );

        let quote_resp = match client.get(&quote_url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    "Jupiter quote request failed for {} via {}: {:?}",
                    mint, base, e
                );
                continue;
            }
        };

        if !quote_resp.status().is_success() {
            let status = quote_resp.status();
            let body = quote_resp.text().await.unwrap_or_default();
            warn!(
                "Jupiter quote HTTP {} for {} via {}: {}",
                status, mint, base, body
            );
            continue;
        }

        let quote_json: serde_json::Value = match quote_resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Jupiter quote parse failed for {} via {}: {:?}",
                    mint, base, e
                );
                continue;
            }
        };

        // Jupiter v6 quote returns { data: [route...] }. Pick the best route (first).
        let route = match quote_json.get("data").and_then(|v| v.as_array()) {
            Some(arr) if !arr.is_empty() => arr[0].clone(),
            _ => {
                warn!("Jupiter quote returned no routes for {} via {}", mint, base);
                continue;
            }
        };

        // If outAmount is missing/zero, treat as no liquidity.
        let out_amount_ok = route
            .get("outAmount")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u128>().ok())
            .map(|n| n > 0)
            .unwrap_or(false);

        if !out_amount_ok {
            warn!(
                "Jupiter route outAmount is zero/missing for {} via {}",
                mint, base
            );
            continue;
        }

        let swap_body = json!({
            // IMPORTANT: /v6/swap expects a single route object from /v6/quote, not the whole response.
            "quoteResponse": route,
            "userPublicKey": treasury.pubkey().to_string(),
            "wrapAndUnwrapSol": true,
            "dynamicComputeUnitLimit": true,
        });

        let swap_url = format!("{}/v6/swap", base);
        let swap_resp = match client.post(&swap_url).json(&swap_body).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    "Jupiter swap request failed for {} via {}: {:?}",
                    mint, base, e
                );
                continue;
            }
        };

        if !swap_resp.status().is_success() {
            let status = swap_resp.status();
            let body = swap_resp.text().await.unwrap_or_default();
            warn!(
                "Jupiter swap HTTP {} for {} via {}: {}",
                status, mint, base, body
            );
            continue;
        }

        let parsed: JupiterSwapResponse = match swap_resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Jupiter swap parse failed for {} via {}: {:?}",
                    mint, base, e
                );
                continue;
            }
        };

        swap_json = Some(parsed);
        break;
    }

    let swap_json = match swap_json {
        Some(v) => v,
        None => return false,
    };

    let tx_bytes = match B64.decode(swap_json.swap_transaction) {
        Ok(b) => b,
        Err(e) => {
            warn!(
                "Jupiter swapTransaction base64 decode failed for {}: {:?}",
                mint, e
            );
            return false;
        }
    };

    let decoded: VersionedTransaction = match bincode::deserialize(&tx_bytes) {
        Ok(t) => t,
        Err(e) => {
            warn!("Jupiter tx deserialize failed for {}: {:?}", mint, e);
            return false;
        }
    };

    // Re-sign with our key (drop any placeholder signatures from API)
    let signed = match VersionedTransaction::try_new(decoded.message, &[treasury.signer_ref()]) {
        Ok(t) => t,
        Err(e) => {
            warn!("Jupiter tx signing failed for {}: {:?}", mint, e);
            return false;
        }
    };

    let config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: None,
        encoding: Some(UiTransactionEncoding::Base64),
        max_retries: None,
        min_context_slot: None,
    };

    let sig = match rpc.rpc.send_transaction_with_config(&signed, config).await {
        Ok(sig) => sig,
        Err(e) => {
            warn!("Jupiter TX send failed for {}: {:?}", mint, e);
            return false;
        }
    };

    // Confirm via signature statuses (best-effort, bounded wait).
    let start = Instant::now();
    loop {
        if start.elapsed().as_secs() > 45 {
            warn!("Jupiter TX confirmation timeout for {}: {}", mint, sig);
            return false;
        }

        match rpc.get_signature_statuses_retry(&[sig]).await {
            Ok(resp) => {
                if let Some(st) = resp.value.get(0).and_then(|v| v.as_ref()) {
                    if st.err.is_some() {
                        warn!("Jupiter TX confirmed with error for {}: {}", mint, sig);
                        return false;
                    }
                    if matches!(
                        st.confirmation_status,
                        Some(
                            solana_transaction_status::TransactionConfirmationStatus::Confirmed
                                | solana_transaction_status::TransactionConfirmationStatus::Finalized
                        )
                    ) {
                        info!("✅ Sold {} via Jupiter! Sig: {}", mint, sig);
                        return true;
                    }
                }
            }
            Err(e) => {
                warn!("Jupiter TX status check failed for {}: {:?}", mint, e);
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Try to sell on Raydium - returns true if successful
async fn try_sell_raydium(
    rpc: Arc<SolanaRpc>,
    raydium: Arc<Raydium>,
    treasury: Arc<Treasury>,
    task: &SellTask,
    wsol_ata: Pubkey,
) -> bool {
    let mint = task.mint;
    let amount = task.amount;
    let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
    let slippage_bps = 9900; // 99% slippage for panic sell

    info!("Trying Raydium for {} ({} tokens)...", mint, amount);

    // Fetch pool from Raydium API
    let url = format!(
        "https://api-v3.raydium.io/pools/info/mint?mint1={}&mint2={}&poolType=all&poolSortField=liquidity&sortType=desc&pageSize=10&page=1",
        mint, sol_mint
    );

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
                let v4_prog = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
                arr.iter()
                    .find(|p| p["programId"].as_str().unwrap_or("") == v4_prog)
                    .and_then(|pool| pool["id"].as_str())
                    .and_then(|id_str| Pubkey::from_str(id_str).ok())
            } else {
                None
            }
        }
        _ => None,
    };

    if let Some(pid) = pool_id {
        if pid != Pubkey::default() {
            info!("Found Raydium pool {} for {}", pid, mint);
            if let Err(e) = raydium.load_pool_from_geyser(&pid).await {
                warn!("Failed to load pool {}: {}", pid, e);
            }
        }
    } else {
        warn!("No Raydium pool found for {}", mint);
        return false;
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
                "Raydium quote: {} tokens -> {} lamports",
                amount, plan.expected_out
            );

            let mut ixs = plan.ixs;
            let raydium_prog =
                Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap();

            for ix in ixs.iter_mut() {
                if ix.program_id == raydium_prog && ix.accounts.len() >= 18 {
                    if ix.accounts[15].pubkey == Pubkey::default() {
                        ix.accounts[15].pubkey = task.ta_pubkey;
                    }
                    if ix.accounts[16].pubkey == Pubkey::default() {
                        ix.accounts[16].pubkey = wsol_ata;
                    }
                    if ix.accounts[17].pubkey == Pubkey::default() {
                        ix.accounts[17].pubkey = treasury.pubkey();
                    }
                }
            }

            let latest_blockhash = match rpc.get_latest_blockhash_retry().await {
                Ok(bh) => bh,
                Err(e) => {
                    warn!("Failed to get blockhash: {:?}", e);
                    return false;
                }
            };

            let mut tx = Transaction::new_with_payer(&ixs, Some(&treasury.pubkey()));
            if tx
                .try_sign(&[treasury.signer_ref()], latest_blockhash)
                .is_err()
            {
                warn!("Failed to sign Raydium tx for {}", mint);
                return false;
            }

            match rpc.rpc.send_and_confirm_transaction(&tx).await {
                Ok(sig) => {
                    info!("✅ Sold {} via Raydium! Sig: {}", mint, sig);
                    true
                }
                Err(e) => {
                    warn!("Raydium TX failed for {}: {:?}", mint, e);
                    false
                }
            }
        }
        Ok(None) => {
            warn!("No Raydium swap plan for {}", mint);
            false
        }
        Err(e) => {
            warn!("Raydium error for {}: {:?}", mint, e);
            false
        }
    }
}

/// Try to sell on Pump.fun - returns true if successful
async fn try_sell_pumpfun(
    rpc: Arc<SolanaRpc>,
    pumpfun: Arc<PumpFunDex>,
    treasury: Arc<Treasury>,
    task: &SellTask,
) -> bool {
    let mint = task.mint;
    let amount = task.amount;
    let sol_mint_str = "So11111111111111111111111111111111111111112";

    info!("Trying Pump.fun for {} ({} tokens)...", mint, amount);

    // Get quote
    let quote = match pumpfun
        .quote_exact_in(&mint.to_string(), sol_mint_str, amount)
        .await
    {
        Ok(Some(q)) => q,
        Ok(None) => {
            warn!("No Pump.fun quote for {}", mint);
            return false;
        }
        Err(e) => {
            warn!("Pump.fun quote error for {}: {:?}", mint, e);
            return false;
        }
    };

    let min_out = quote.amount_out / 100; // 1% min for panic sell
    info!(
        "Pump.fun quote: {} tokens -> {} lamports",
        amount, quote.amount_out
    );

    match pumpfun
        .build_swap_ix_async(&mint.to_string(), sol_mint_str, amount, min_out, None)
        .await
    {
        Ok(ixs) if !ixs.is_empty() => {
            let latest_blockhash = match rpc.get_latest_blockhash_retry().await {
                Ok(bh) => bh,
                Err(e) => {
                    warn!("Failed to get blockhash: {:?}", e);
                    return false;
                }
            };

            let mut tx = Transaction::new_with_payer(&ixs, Some(&treasury.pubkey()));
            if tx
                .try_sign(&[treasury.signer_ref()], latest_blockhash)
                .is_err()
            {
                warn!("Failed to sign Pump.fun tx for {}", mint);
                return false;
            }

            match rpc.rpc.send_and_confirm_transaction(&tx).await {
                Ok(sig) => {
                    info!("✅ Sold {} via Pump.fun! Sig: {}", mint, sig);
                    true
                }
                Err(e) => {
                    warn!("Pump.fun TX failed for {}: {:?}", mint, e);
                    false
                }
            }
        }
        Ok(_) => {
            warn!("No Pump.fun instructions for {}", mint);
            false
        }
        Err(e) => {
            warn!("Pump.fun build error for {}: {:?}", mint, e);
            false
        }
    }
}

/// Try ALL DEXes before giving up. Returns SellResult indicating if burn is needed.
async fn sell_token(
    rpc: Arc<SolanaRpc>,
    raydium: Arc<Raydium>,
    pumpfun: Arc<PumpFunDex>,
    treasury: Arc<Treasury>,
    task: SellTask,
    wsol_ata: Pubkey,
    jupiter_base_url: Arc<String>,
) -> SellResult {
    let mint = task.mint;
    let mut errors = Vec::new();

    // Strategy: Try the most likely DEX first based on mint pattern
    let is_pumpfun_token = is_pumpfun_mint(&mint);

    if is_pumpfun_token {
        // Pump.fun token: try Pump.fun first, then Raydium (for migrated tokens)
        info!(
            "{} looks like a Pump.fun token, trying Pump.fun first...",
            mint
        );

        if try_sell_pumpfun(rpc.clone(), pumpfun.clone(), treasury.clone(), &task).await {
            return SellResult::Sold;
        }
        errors.push("Pump.fun failed");

        info!("Pump.fun failed, trying Raydium (token may have migrated)...");
        if try_sell_raydium(
            rpc.clone(),
            raydium.clone(),
            treasury.clone(),
            &task,
            wsol_ata,
        )
        .await
        {
            return SellResult::Sold;
        }
        errors.push("Raydium failed");

        info!("Pump.fun + Raydium failed, trying Jupiter fallback...");
        if try_sell_jupiter(
            rpc.clone(),
            treasury.clone(),
            &task,
            jupiter_base_url.as_str(),
        )
        .await
        {
            return SellResult::Sold;
        }
        errors.push("Jupiter failed");
    } else {
        // Non-Pump.fun token: try Raydium first, then Pump.fun as fallback
        info!("{} trying Raydium first...", mint);

        if try_sell_raydium(
            rpc.clone(),
            raydium.clone(),
            treasury.clone(),
            &task,
            wsol_ata,
        )
        .await
        {
            return SellResult::Sold;
        }
        errors.push("Raydium failed");

        info!("Raydium failed, trying Pump.fun as fallback...");
        if try_sell_pumpfun(rpc.clone(), pumpfun.clone(), treasury.clone(), &task).await {
            return SellResult::Sold;
        }
        errors.push("Pump.fun failed");

        info!("Raydium + Pump.fun failed, trying Jupiter fallback...");
        if try_sell_jupiter(
            rpc.clone(),
            treasury.clone(),
            &task,
            jupiter_base_url.as_str(),
        )
        .await
        {
            return SellResult::Sold;
        }
        errors.push("Jupiter failed");
    }

    // All DEXes failed - return for user confirmation
    let reason = errors.join(", ");
    warn!(
        "❌ All DEXes failed for {} ({} tokens): {}",
        mint, task.amount, reason
    );

    SellResult::NeedsConfirmation(task, reason)
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

    let mut raydium = Raydium::new(rpc.clone());
    raydium.set_user_authority(treasury.pubkey());
    let raydium = Arc::new(raydium);

    // Initialize Pump.fun with user authority for selling
    let mut pumpfun = PumpFunDex::new(rpc.clone()).expect("Failed to create PumpFunDex");
    pumpfun.set_user_authority(treasury.pubkey());
    let pumpfun = Arc::new(pumpfun);

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
    let mut tasks: Vec<SellTask> = Vec::new();

    for ta in token_accounts {
        let data = ta.account.data;
        let ta_pubkey = match Pubkey::from_str(&ta.pubkey) {
            Ok(p) => p,
            Err(_) => continue,
        };

        let decode_raw_token_account = |b: String| -> Option<(Pubkey, u64)> {
            let bytes = bs58::decode(b).into_vec().ok()?;
            if bytes.len() < 72 {
                return None;
            }

            // SPL Token account layout: freeze state at offset 108 (if present)
            if bytes.len() >= 109 && bytes[108] == 2 {
                return None;
            }

            let mint_bytes: [u8; 32] = bytes[0..32].try_into().ok()?;
            let mint = Pubkey::new_from_array(mint_bytes);
            let amount_bytes: [u8; 8] = bytes[64..72].try_into().ok()?;
            let amount = u64::from_le_bytes(amount_bytes);
            Some((mint, amount))
        };

        let result: Option<(Pubkey, u64)> = match data {
            solana_account_decoder::UiAccountData::Binary(b, _) => decode_raw_token_account(b),
            solana_account_decoder::UiAccountData::LegacyBinary(b) => decode_raw_token_account(b),
            solana_account_decoder::UiAccountData::Json(parsed) => {
                if let serde_json::Value::Object(info) = parsed.parsed {
                    let Some(info_obj) = info.get("info") else {
                        return None;
                    };

                    let is_frozen = info_obj
                        .get("state")
                        .and_then(|v| v.as_str())
                        .map(|s| s.eq_ignore_ascii_case("frozen"))
                        .unwrap_or(false);

                    if is_frozen {
                        None
                    } else {
                        let Some(mint_str) = info_obj.get("mint").and_then(|v| v.as_str()) else {
                            return None;
                        };

                        let amount_str = info_obj
                            .get("tokenAmount")
                            .and_then(|v| v.get("amount"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("0");

                        let Ok(mint) = Pubkey::from_str(mint_str) else {
                            return None;
                        };

                        let amount = u64::from_str(amount_str).unwrap_or(0);
                        Some((mint, amount))
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
        update_risk_state()?;
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
    let force_burn = args.force_burn;
    let jupiter_base_url = Arc::new(
        args.jupiter_base_url
            .clone()
            .unwrap_or_else(|| "https://quote-api.jup.ag".to_string()),
    );

    // Collect results to handle burns separately
    let results: Vec<SellResult> = stream::iter(tasks)
        .map(|task| {
            let rpc = rpc.clone();
            let raydium = raydium.clone();
            let pumpfun = pumpfun.clone();
            let treasury = treasury.clone();
            let jupiter_base_url = jupiter_base_url.clone();
            async move {
                sell_token(
                    rpc,
                    raydium,
                    pumpfun,
                    treasury,
                    task,
                    wsol_ata,
                    jupiter_base_url,
                )
                .await
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // Collect tokens that need burn confirmation
    let needs_burn: Vec<(SellTask, String)> = results
        .into_iter()
        .filter_map(|r| match r {
            SellResult::NeedsConfirmation(task, reason) => Some((task, reason)),
            SellResult::Sold => None,
        })
        .collect();

    // Handle burn confirmations
    if !needs_burn.is_empty() {
        println!("\n{}", "=".repeat(60));
        println!("⚠️  WARNING: The following tokens could NOT be sold on any DEX!");
        println!("These tokens have NO LIQUIDITY and can only be BURNED to recover rent (~0.002 SOL each).");
        println!("{}", "=".repeat(60));

        for (task, reason) in &needs_burn {
            println!(
                "\n  Mint: {}\n  Amount: {} tokens\n  Reason: {}",
                task.mint, task.amount, reason
            );
        }

        println!("\n{}", "=".repeat(60));

        if force_burn {
            println!(
                "--force-burn flag set, burning all {} tokens...",
                needs_burn.len()
            );
        } else {
            print!(
                "\nDo you want to BURN these {} token(s) and close accounts? [y/N]: ",
                needs_burn.len()
            );
            io::stdout().flush()?;

            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim().to_lowercase();

            if input != "y" && input != "yes" {
                println!("Burn cancelled. Tokens remain in wallet.");
                info!("User declined burn for {} tokens", needs_burn.len());
            } else {
                println!("Burning {} tokens...", needs_burn.len());
                for (task, _) in needs_burn {
                    if let Err(e) =
                        burn_and_close_account(rpc.clone(), treasury.clone(), &task).await
                    {
                        warn!("Failed to burn {}: {:?}", task.mint, e);
                    }
                }
            }
        }
    }

    info!("All sells completed. Unwrapping WSOL...");
    let _ = treasury.unwrap_wsol(&rpc, None).await;

    // Update risk state
    update_risk_state()?;

    Ok(())
}

fn update_risk_state() -> anyhow::Result<()> {
    let path = PathBuf::from("state/risk_state.json");
    if !path.exists() {
        info!("No risk state file found at state/risk_state.json, skipping update.");
        return Ok(());
    }

    let content = fs::read_to_string(&path)?;
    let mut json: serde_json::Value = serde_json::from_str(&content)?;

    if let Some(obj) = json.as_object_mut() {
        if let Some(open_pos) = obj.get_mut("open_positions") {
            info!("Clearing open positions in risk state...");
            *open_pos = serde_json::json!([]);
        }
    }

    let new_content = serde_json::to_string_pretty(&json)?;
    let mut file = fs::File::create(&path)?;
    file.write_all(new_content.as_bytes())?;
    info!("Risk state updated successfully.");
    Ok(())
}
