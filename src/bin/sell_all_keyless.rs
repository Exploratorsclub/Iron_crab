use base64::Engine;
use clap::Parser;
use ironcrab::config::Config;
use ironcrab::ipc::{
    DecisionOutcome, DecisionRecord, ExplicitAmount, IntentOrigin, IntentTier, TradeExecutionConstraints,
    TradeIntent, TradeResources, TradingRegime,
};
use ironcrab::nats::{
    NatsClient, NatsConfig, TOPIC_DECISION_RECORDS, TOPIC_EXECUTION_RESULTS, TOPIC_TRADE_INTENTS,
};
use ironcrab::solana::dex::pumpfun::{BondingCurveState, PumpFunDex};
use ironcrab::solana::dex::pumpfun_amm::PumpFunAmmDex;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::token_utils::get_token_decimals_or_default;
use ironcrab::storage::jsonl_writer::{JsonlWriter, JsonlWriterConfig};
use solana_account_decoder::UiAccountData;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::pubkey::Pubkey as SplProgPubkey;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about = "Keyless liquidation tool (publishes SELL TradeIntents to NATS)")]
struct Args {
    #[arg(short, long, default_value = "my_config.server.toml")]
    config: PathBuf,

    /// Override RPC URL (e.g. https://api.mainnet-beta.solana.com)
    #[arg(long)]
    rpc_url: Option<String>,

    /// Wallet owner pubkey to liquidate (required; keyless tool)
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

    /// Wait for DecisionRecords/ExecutionResults and print a final summary
    #[arg(long, default_value_t = true)]
    wait: bool,

    /// Wait timeout (seconds) if --wait
    #[arg(long, default_value_t = 180)]
    wait_timeout_secs: u64,

    /// JSONL log dir (default: trade_logs/liquidations)
    #[arg(long)]
    log_dir: Option<PathBuf>,
}

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

fn decode_token_account_mint_amount(data: UiAccountData) -> Option<(Pubkey, u64)> {
    // Prefer JSON-parsed response if present.
    if let UiAccountData::Json(parsed) = data {
        if let serde_json::Value::Object(parsed_obj) = parsed.parsed {
            let info = parsed_obj.get("info")?;
            let state = info.get("state").and_then(|v| v.as_str()).unwrap_or("");
            if state.eq_ignore_ascii_case("frozen") {
                return None;
            }

            let mint_str = info.get("mint")?.as_str()?;
            let amount_str = info
                .get("tokenAmount")?
                .get("amount")?
                .as_str()
                .unwrap_or("0");

            let mint = Pubkey::from_str(mint_str).ok()?;
            let amount = u64::from_str(amount_str).ok()?;
            return Some((mint, amount));
        }
        return None;
    }

    // Fallback: decode raw SPL-token account bytes.
    // Token account layout: mint[0..32], amount at [64..72]
    let bytes = match data {
        UiAccountData::Binary(b, _) => base64::engine::general_purpose::STANDARD.decode(b).ok()?,
        UiAccountData::LegacyBinary(b) => base64::engine::general_purpose::STANDARD.decode(b).ok()?,
        _ => return None,
    };

    if bytes.len() < 72 {
        return None;
    }

    // Freeze state is at offset 108 for classic token accounts (if present)
    if bytes.len() >= 109 && bytes[108] == 2 {
        return None;
    }

    let mint_bytes: [u8; 32] = bytes[0..32].try_into().ok()?;
    let amount_bytes: [u8; 8] = bytes[64..72].try_into().ok()?;

    let mint = Pubkey::new_from_array(mint_bytes);
    let amount = u64::from_le_bytes(amount_bytes);
    Some((mint, amount))
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
        info!(rpc_url = %url, "Overriding RPC URL");
        cfg.solana.rpc_url = url;
    }

    let owner = Pubkey::from_str(&args.owner_pubkey)
        .map_err(|e| anyhow::anyhow!("invalid --owner-pubkey: {e}"))?;

    info!(
        run_id = %run_id,
        owner = %owner,
        rpc_url = %cfg.solana.rpc_url,
        "Starting keyless sell-all liquidation"
    );

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
    let pumpfun = PumpFunDex::new(Arc::clone(&rpc))?;
    let pump_amm = PumpFunAmmDex::new(
        Arc::clone(&rpc),
        cfg.solana.rpc_url.clone(),
        cfg.solana.helius_rpc_url.clone(),
    );

    info!("Refreshing Raydium pools (for route discovery)...");
    raydium.refresh_pools().await?;

    // Fetch all token accounts (Token Program AND Token-2022)
    let token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")?;
    let token_2022_program_id = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;

    let mut token_accounts = rpc
        .rpc
        .get_token_accounts_by_owner(&owner, TokenAccountsFilter::ProgramId(token_program_id))
        .await?;

    if let Ok(mut accounts_2022) = rpc
        .rpc
        .get_token_accounts_by_owner(&owner, TokenAccountsFilter::ProgramId(token_2022_program_id))
        .await
    {
        token_accounts.append(&mut accounts_2022);
    }

    info!(count = token_accounts.len(), "Found token accounts total");

    let sol_mint = Pubkey::from_str(SOL_MINT)?;

    let mut tasks: Vec<SellTask> = Vec::new();

    for ta in token_accounts {
        let ta_pubkey = match Pubkey::from_str(&ta.pubkey) {
            Ok(p) => p,
            Err(e) => {
                warn!(pubkey = %ta.pubkey, error = %e, "Skipping token account: invalid pubkey");
                continue;
            }
        };
        let (mint, amount) = match decode_token_account_mint_amount(ta.account.data) {
            Some(v) => v,
            None => continue,
        };

        if mint == sol_mint || amount == 0 {
            continue;
        }

        let token_program = match token_program_for_mint(rpc.as_ref(), &mint).await {
            Ok(p) => p,
            Err(e) => {
                warn!(mint = %mint, error = %e, "Skipping mint: cannot determine token program");
                continue;
            }
        };

        let ata = ata_for_owner_mint(&owner, &mint, &token_program);
        if ta_pubkey != ata {
            warn!(
                mint = %mint,
                token_account = %ta_pubkey,
                ata = %ata,
                "Skipping non-ATA token account (engine sells from ATA)"
            );
            continue;
        }

        let decimals = get_token_decimals_or_default(rpc.as_ref(), &mint).await;

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

    let mut intent_to_mint: HashMap<String, Pubkey> = HashMap::new();
    let mut published = 0usize;
    let mut skipped = 0usize;

    for task in &tasks {
        let mint = task.mint;
        let amount_in = task.amount_raw;

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
        };

        let mut min_out_sol: Option<u64> = None;

        // Prefer Pump.fun for non-migrated curve tokens.
        if let Ok(Some(q)) = pumpfun
            .quote_exact_in(&mint.to_string(), SOL_MINT, amount_in)
            .await
        {
            // Pump.fun execution needs metadata.creator.
            if let Some(bc) = q.route.first().and_then(|s| Pubkey::from_str(s).ok()) {
                if let Ok(acct) = rpc.rpc.get_account(&bc).await {
                    if let Ok(state) = BondingCurveState::parse(&acct.data) {
                        metadata.insert("creator".to_string(), state.creator.to_string());
                    }
                }
            }

            if metadata.contains_key("creator") {
                metadata.insert("dex".to_string(), "pumpfun".to_string());
                min_out_sol = Some(apply_slippage_min_out(q.amount_out, args.max_slippage_bps));
            }
        }

        // PumpSwap / Pump.fun AMM (migrated tokens).
        if min_out_sol.is_none() {
            if let Ok(Some(q)) = pump_amm
                .quote_exact_in(&mint.to_string(), SOL_MINT, amount_in)
                .await
            {
                if let Ok(Some(pool_accounts)) = pump_amm.pool_accounts_v1_for_base_mint(mint).await
                {
                    if let Some(pool_id) = q.route.first().cloned() {
                        metadata.insert("dex".to_string(), "pump_amm".to_string());
                        resources.pools = vec![pool_id];
                        resources.accounts = pool_accounts
                            .iter()
                            .map(|p| p.to_string())
                            .collect();
                        min_out_sol =
                            Some(apply_slippage_min_out(q.amount_out, args.max_slippage_bps));
                    }
                }
            }
        }

        // Fallback to Raydium
        if min_out_sol.is_none() {
            if let Ok(Some(q)) = raydium
                .quote_exact_in(&mint.to_string(), SOL_MINT, amount_in)
                .await
            {
                if let Some(pool_id) = q.route.first().cloned() {
                    metadata.insert("dex".to_string(), "raydium".to_string());
                    resources.pools = vec![pool_id];
                    min_out_sol = Some(apply_slippage_min_out(q.amount_out, args.max_slippage_bps));
                }
            }
        }

        let Some(min_out) = min_out_sol else {
            skipped += 1;
            warn!(mint = %mint, "No supported liquidation route found; skipping intent publish");
            continue;
        };

        let intent_id = format!("liquidation-{}", Uuid::new_v4());
        let mut intent = TradeIntent::new_sell(
            "sell-all",
            build,
            &run_id,
            intent_id.clone(),
            "sell-all",
            IntentTier::Tier0,
            IntentOrigin::StrategyA,
            mint.to_string(),
            task.mint_decimals,
            SOL_MINT.to_string(),
            amount_in,
            0,
            args.max_slippage_bps,
            TradingRegime::NotApplicable,
        );
        intent.resources = resources;
        intent.execution = Some(TradeExecutionConstraints {
            min_out: Some(ExplicitAmount::new(min_out, 9)),
        });
        intent.ttl_ms = Some(args.ttl_ms);
        intent.metadata.extend(metadata);

        jsonl.write(&serde_json::json!({"kind": "trade_intent", "intent": intent}))?;

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

    for (intent_id, mint) in intent_to_mint {
        let outcome = outcomes.get(&intent_id).copied().unwrap_or(LiquidationOutcome::Pending);
        info!(intent_id = %intent_id, mint = %mint, outcome = ?outcome, "Liquidation outcome");
    }

    Ok(())
}
