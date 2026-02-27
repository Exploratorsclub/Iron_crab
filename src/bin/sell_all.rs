//! Deprecated legacy implementation.
//!
//! The architecture-aligned, keyless liquidation tool lives in `sell_all_keyless.rs`
//! and publishes `TradeIntent`s to NATS (execution-engine is the only signer).

fn main() {
    eprintln!(
        "Deprecated: this legacy sell-all implementation is no longer used. \
Run the `sell-all` binary (wired to src/bin/sell_all_keyless.rs)."
    );
    std::process::exit(1);
}

/*

use clap::Parser;
use ironcrab::config::Config;
use ironcrab::ipc::{
    DecisionOutcome, DecisionRecord, ExplicitAmount, IntentOrigin, IntentTier, TradeExecutionConstraints,
    TradeIntent, TradeResources, TradeSide, TradingRegime,
};
use ironcrab::nats::{
    NatsClient, NatsConfig, TOPIC_DECISION_RECORDS, TOPIC_EXECUTION_RESULTS, TOPIC_TRADE_INTENTS,
};
use ironcrab::solana::dex::pumpfun::PumpFunDex;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::token_utils::get_token_decimals_or_default;
use ironcrab::storage::jsonl_writer::{JsonlWriter, JsonlWriterConfig};
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::bs58;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use uuid::Uuid;

use spl_token::solana_program::pubkey::Pubkey as SplProgPubkey;

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

        /// Wallet owner pubkey to liquidate (required; keyless tool).
        #[arg(long)]
        owner_pubkey: String,

        /// NATS URL (default: $NATS_URL or nats://localhost:4222)
        #[arg(long, default_value = "")]
        nats_url: String,

        /// Max slippage for liquidation intents (bps). Default: 9900 (panic sell)
        #[arg(long, default_value_t = 9900)]
        max_slippage_bps: u32,

        /// TTL for intents (ms). Default: 60000
        #[arg(long, default_value_t = 60_000)]
        ttl_ms: u64,

        /// Optional: wait for DecisionRecords/ExecutionResults and print a final summary.
        #[arg(long, default_value_t = true)]
        wait: bool,

        /// Wait timeout (seconds) if --wait
        #[arg(long, default_value_t = 180)]
        wait_timeout_secs: u64,

        /// JSONL log dir (default: trade_logs/liquidations)
        #[arg(long)]
        log_dir: Option<PathBuf>,
        accounts: vec![

    #[derive(Debug, Clone)]
    struct SellTask {
        mint: Pubkey,
        amount_raw: u64,
        token_account: Pubkey,
        token_program: Pubkey,
        mint_decimals: u8,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum LiquidationOutcome {
        Pending,
        Rejected,
        SimFailed,
        Sent,
        Confirmed,
        Failed,
        Timeout,
    }

    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

    #[inline]
    fn sdk_to_spl(pk: &Pubkey) -> SplProgPubkey {
        SplProgPubkey::new_from_array(pk.to_bytes())
    }

    #[inline]
    fn spl_to_sdk(pk: &SplProgPubkey) -> Pubkey {
        Pubkey::new_from_array(pk.to_bytes())
    }

    fn apply_slippage_min_out(quoted_out: u64, slippage_bps: u32) -> u64 {
        let keep_bps = 10_000u64.saturating_sub(slippage_bps as u64);
        ((quoted_out as u128) * (keep_bps as u128) / 10_000u128) as u64
    }

    fn ensure_keyless_or_exit() {
        let key_vars = [
            "IRONCRAB_KEYPAIR_JSON",
            "IRONCRAB_KEYPAIR_B64",
            "IRONCRAB_KEYPAIR_PATH",
            "IRONCRAB_KEYPAIR_BASE58",
        ];
        if key_vars.iter().any(|v| std::env::var(v).is_ok()) {
            error!(
                "ERROR: Wallet key environment variables detected! sell-all is KEYLESS per architecture."
            );
            error!("Only execution-engine should have access to wallet keys.");
            std::process::exit(1);
        }
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
            if let Err(e) = raydium.load_pool_from_rpc_fallback(&pid).await {
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("sell_all=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
        )
        .init();

    let args = Args::parse();
    let mut cfg = Config::load(&args.config)?;

    ensure_keyless_or_exit();

    let run_id = Uuid::new_v4().to_string();
    let build = env!("CARGO_PKG_VERSION");

    if let Some(url) = args.rpc_url {
        info!("Overriding RPC URL: {}", url);
        cfg.solana.rpc_url = url;
    }

    let owner = Pubkey::from_str(&args.owner_pubkey)
        .map_err(|e| anyhow::anyhow!("invalid --owner-pubkey: {e}"))?;

    info!(run_id = %run_id, owner = %owner, rpc_url = %cfg.solana.rpc_url, "Starting keyless sell-all liquidation");

    let rpc = Arc::new(SolanaRpc::from_cfg(&cfg.solana));

    let log_dir = args
        .log_dir
        .unwrap_or_else(|| PathBuf::from("trade_logs/liquidations"));
    let jsonl = JsonlWriter::new(JsonlWriterConfig::new("liquidations").with_log_dir(&log_dir))?;

    // Setup NATS
    let nats_url = if args.nats_url.trim().is_empty() {
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
    } else {
        args.nats_url.clone()
    };
    let mut nats = NatsClient::new(NatsConfig::new(&nats_url, "sell-all"));
    nats.connect().await?;
    info!(nats_url = %nats_url, "Connected to NATS");

    // Initialize DEX connectors (keyless) for route discovery only
    let raydium = Raydium::new(Arc::clone(&rpc));
    let pumpfun = PumpFunDex::new(Arc::clone(&rpc), None)?;

    // Raydium requires pool snapshots to quote + provide pool id
    info!("Refreshing Raydium pools (for route discovery)...");
    raydium.refresh_pools().await?;

    // Fetch all token accounts (Token Program AND Token-2022)
    let token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    let token_2022_program_id =
        Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap();

    let mut token_accounts = rpc
        .rpc
        .get_token_accounts_by_owner(
            &owner,
            TokenAccountsFilter::ProgramId(token_program_id),
        )
        .await?;

    // Also fetch Token-2022 accounts
    if let Ok(mut accounts_2022) = rpc
        .rpc
        .get_token_accounts_by_owner(
            &owner,
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
                    match info.get("info") {
                        Some(info_obj) => {
                            let is_frozen = info_obj
                                .get("state")
                                .and_then(|v| v.as_str())
                                .map(|s| s.eq_ignore_ascii_case("frozen"))
                                .unwrap_or(false);

                            if is_frozen {
                                None
                            } else {
                                if let Some(mint_str) = info_obj.get("mint").and_then(|v| v.as_str()) {
                                    let amount_str = info_obj
                                        .get("tokenAmount")
                                        .and_then(|v| v.get("amount"))
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("0");

                                    match Pubkey::from_str(mint_str) {
                                        Ok(mint) => {
                                            let amount = u64::from_str(amount_str).unwrap_or(0);
                                            Some((mint, amount))
                                        }
                                        Err(_) => None,
                                    }
                                } else {
                                    None
                                }
                            }
                        }
                        None => None,
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

        // Determine mint decimals + token program, and only act on the ATA (engine executes from ATA)
        let token_program = match token_program_for_mint(&rpc, &mint).await {
            Ok(p) => p,
            Err(e) => {
                warn!(mint = %mint, error = %e, "Skipping mint: cannot determine token program");
                continue;
            }
        };

        let ata = ata_for_owner_mint(&owner, &mint, &token_program);
        if ta_pubkey != ata {
            // Avoid false-positive liquidation: engine will sell from ATA only.
            warn!(mint = %mint, token_account = %ta_pubkey, ata = %ata, "Skipping non-ATA token account (engine sells from ATA)");
            continue;
        }

        let decimals = get_token_decimals_or_default(&rpc, &mint, None).await;

        info!(mint = %mint, amount_raw = amount, decimals, ata = %ata, "Queuing liquidation");
        tasks.push(SellTask {
            mint,
            amount_raw: amount,
            token_account: ta_pubkey,
            token_program,
            mint_decimals: decimals,
        });
    }

    if tasks.is_empty() {
        info!("No sellable ATA balances found.");
        return Ok(());
    }

    // Publish liquidation intents
    let mut intent_to_mint: HashMap<String, Pubkey> = HashMap::new();
    let mut published = 0usize;
    let mut skipped = 0usize;

    for task in &tasks {
        let mint = task.mint;
        let amount_in = task.amount_raw;

        // Discover route + min_out
        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("purpose".to_string(), "liquidation".to_string());
        metadata.insert("sell_all".to_string(), "true".to_string());
        metadata.insert("mint_decimals".to_string(), task.mint_decimals.to_string());
        metadata.insert("token_account".to_string(), task.token_account.to_string());
        metadata.insert("token_program".to_string(), task.token_program.to_string());

        let mut resources = TradeResources {
            input_mint: mint.to_string(),
            output_mint: SOL_MINT.to_string(),
            pools: vec![],
            accounts: vec![task.token_account.to_string()],
            token_program: Some(task.token_program.to_string()),
        };

        // Prefer Pump.fun for non-migrated curve tokens.
        // If migrated, Pump.fun quote returns None and we try Raydium.
        let mut dex: Option<&'static str> = None;
        let mut min_out_sol: Option<u64> = None;

        if let Ok(Some(q)) = pumpfun
            .quote_exact_in(&mint.to_string(), SOL_MINT, amount_in)
            .await
        {
            // We need creator for Pump.fun tx build (execution-engine requires metadata.creator).
            // The quote route includes the bonding curve account, from which we can parse creator.
            if let Some(bc) = q.route.first().and_then(|s| Pubkey::from_str(s).ok()) {
                if let Ok(acct) = rpc.rpc.get_account(&bc).await {
                    if let Ok(state) = ironcrab::solana::dex::pumpfun::BondingCurveState::parse(&acct.data) {
                        metadata.insert("creator".to_string(), state.creator.to_string());
                    }
                }
            }

            if metadata.get("creator").is_some() {
                dex = Some("pumpfun");
                let quoted_out = q.amount_out;
                let min_out = apply_slippage_min_out(quoted_out, args.max_slippage_bps);
                min_out_sol = Some(min_out);
                metadata.insert("dex".to_string(), "pumpfun".to_string());
            }
        }

        if dex.is_none() {
            if let Ok(Some(q)) = raydium
                .quote_exact_in(&mint.to_string(), SOL_MINT, amount_in)
                .await
            {
                if let Some(pool_id) = q.route.first().cloned() {
                    dex = Some("raydium");
                    metadata.insert("dex".to_string(), "raydium".to_string());
                    resources.pools = vec![pool_id];
                    let quoted_out = q.amount_out;
                    let min_out = apply_slippage_min_out(quoted_out, args.max_slippage_bps);
                    min_out_sol = Some(min_out);
                }
            }
        }

        let Some(min_out) = min_out_sol else {
            skipped += 1;
            warn!(mint = %mint, "No supported liquidation route found; skipping intent publish");
            continue;
        };

        let intent_id = format!("liquidation-{}", Uuid::new_v4());
        let mut intent = TradeIntent::new(
            "sell-all",
            build,
            &run_id,
            intent_id.clone(),
            "sell-all",
            IntentTier::Tier0,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(amount_in, task.mint_decimals),
            resources,
            0,
            args.max_slippage_bps,
            TradeSide::Sell,
            TradingRegime::NotApplicable,
        );
        intent.ttl_ms = Some(args.ttl_ms);
        intent.metadata.extend(metadata);
        intent.execution = Some(TradeExecutionConstraints {
            min_out: Some(ExplicitAmount::new(min_out, 9)),
        });

        jsonl.write(&serde_json::json!({
            "kind": "trade_intent",
            "intent": intent,
        }))?;

        let ok = nats.publish(TOPIC_TRADE_INTENTS, &intent).await?;
        if !ok {
            anyhow::bail!("NATS publish dropped/failed topic={}", TOPIC_TRADE_INTENTS);
        }

        intent_to_mint.insert(intent_id, mint);
        published += 1;
    }

    info!(published, skipped, "Liquidation intents published");
    if !args.wait || intent_to_mint.is_empty() {
        return Ok(());
    }

    // Wait for results
    let mut pending: HashSet<String> = intent_to_mint.keys().cloned().collect();
    let mut outcomes: HashMap<String, LiquidationOutcome> = pending
        .iter()
        .map(|id| (id.clone(), LiquidationOutcome::Pending))
        .collect();

    let mut exec_sub = nats.subscribe(TOPIC_EXECUTION_RESULTS).await?;
    let mut dec_sub = nats.subscribe(TOPIC_DECISION_RECORDS).await?;

    let deadline = Instant::now() + Duration::from_secs(args.wait_timeout_secs);
    while !pending.is_empty() && Instant::now() < deadline {
        tokio::select! {
            msg = exec_sub.next() => {
                let Some(msg) = msg else { continue; };
                if let Ok(exec) = serde_json::from_slice::<ironcrab::ipc::ExecutionResult>(&msg.payload) {
                    if !pending.contains(&exec.intent_id) {
                        continue;
                    }
                    let o = match exec.status {
                        ironcrab::ipc::ExecutionStatus::Sent => LiquidationOutcome::Sent,
                        ironcrab::ipc::ExecutionStatus::Confirmed => LiquidationOutcome::Confirmed,
                        ironcrab::ipc::ExecutionStatus::Failed => LiquidationOutcome::Failed,
                        ironcrab::ipc::ExecutionStatus::Timeout => LiquidationOutcome::Timeout,
                    };
                    outcomes.insert(exec.intent_id.clone(), o);
                    if matches!(o, LiquidationOutcome::Confirmed | LiquidationOutcome::Failed | LiquidationOutcome::Timeout) {
                        pending.remove(&exec.intent_id);
                    }
                    jsonl.write(&serde_json::json!({"kind":"execution_result","exec":exec}))?;
                }
            }
            msg = dec_sub.next() => {
                let Some(msg) = msg else { continue; };
                if let Ok(dec) = serde_json::from_slice::<DecisionRecord>(&msg.payload) {
                    if !pending.contains(&dec.intent_id) {
                        continue;
                    }
                    match dec.outcome {
                        DecisionOutcome::Rejected | DecisionOutcome::Expired => {
                            outcomes.insert(dec.intent_id.clone(), LiquidationOutcome::Rejected);
                            pending.remove(&dec.intent_id);
                        }
                        DecisionOutcome::SimFailed => {
                            outcomes.insert(dec.intent_id.clone(), LiquidationOutcome::SimFailed);
                            pending.remove(&dec.intent_id);
                        }
                        _ => {}
                    }
                    jsonl.write(&serde_json::json!({"kind":"decision_record","decision":dec}))?;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }

    // Final summary
    for (intent_id, mint) in intent_to_mint {
        let outcome = outcomes.get(&intent_id).copied().unwrap_or(LiquidationOutcome::Pending);
        info!(intent_id = %intent_id, mint = %mint, outcome = ?outcome, "Liquidation outcome");
    }

    Ok(())

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

*/
