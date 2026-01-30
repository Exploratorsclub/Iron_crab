//! execution-engine binary – Single Signer / Execution Plane
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.3
//!
//! Responsibilities:
//! - ONLY process allowed to load wallet keys
//! - Subscribe to TradeIntents from NATS
//! - Global Arbitration (EV × urgency × deadline)
//! - Capital Locks + Resource Locks
//! - Pipeline: Intent → Arbitrate → Plan → Simulate → Send → Confirm
//! - Emit DecisionRecords + ExecutionResults (even on reject)
//! - Write JSONL for replay/forensics
//!
//! P0 Requirements:
//! - Simulate-gated: simulation fail = never send
//! - Decision Records for every intent
//! - Reason-coded rejects
//! - No silent failure: all errors logged with reason code (DoD O)
//!
//! P1: State Persistence (DoD K)
//! - State survives restarts via StateSnapshot
//! - Idempotency store persisted and loaded
//! - Daily loss tracking persisted

use anyhow::Result;
use base64::Engine as _;
use clap::Parser;
use serde::{Deserialize, Serialize};
use solana_account_decoder::UiAccountData;
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{
    RpcSendTransactionConfig, RpcSimulateTransactionConfig, RpcTransactionConfig,
};
use solana_client::rpc_request::TokenAccountsFilter;
use solana_commitment_config::CommitmentLevel;
use solana_message::{v0, AddressLookupTableAccount, VersionedMessage};
use solana_sdk::bs58;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding, UiTransactionTokenBalance,
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::config::Config as AppConfig;
// cache_geyser removed - execution-engine now subscribes to PoolCacheUpdates from market-data via NATS
use ironcrab::execution::live_pool_cache::{
    create_shared_cache, CachedPoolState, SharedLivePoolCache,
};
use ironcrab::execution::quote_calculator;
use ironcrab::execution::tx_builder;
use ironcrab::execution::wsol_manager::{WsolManager, WsolManagerConfig};
use ironcrab::ipc::{
    CheckResult, ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, DecisionOutcome,
    DecisionRecord, ExecutionResult, ExecutionStatus, ExplicitAmount, FairnessPolicy, FeePolicy,
    FillStatus, FillUnavailableReason, IntentOrigin, IntentTier, KillSwitchContext, PoolCacheUpdate,
    PoolCacheUpdateType, PriorityFeePercentiles, RecordHeader, RejectReason, SimulationResult,
    TradeExecutionConstraints, TradeIntent, TradeResources, TradeSide, TradingRegime,
};
use ironcrab::ipc::{ControlRequest, ControlRequestKind};
use ironcrab::metrics::{
    record_recent_trade, serve_metrics, RecentTrade, ACTIVE_CAPITAL_LOCKS, ACTIVE_RESOURCE_LOCKS,
    AVAILABLE_SOL_LAMPORTS, INTENTS_EXECUTED_TOTAL, INTENTS_RECEIVED_TOTAL, INTENTS_REJECTED_TOTAL,
    JITO_BUNDLES_LANDED_TOTAL, JITO_BUNDLES_REJECTED_TOTAL, JITO_BUNDLES_SUBMITTED_TOTAL,
    JITO_BUNDLES_TIMEOUT_TOTAL, JITO_TIP_LAMPORTS_TOTAL, NATS_MESSAGES_RECEIVED_TOTAL,
    OPEN_POSITIONS_GAUGE, REJECT_CAPITAL_LOCK, REJECT_DUPLICATE, REJECT_RESOURCE_LOCK,
    REJECT_SEND_FAILED, REJECT_SIMULATION_FAIL, SIMULATION_FAILURES_TOTAL, TX_CONFIRMED_TOTAL,
    TX_CONFIRM_TIMEOUT_TOTAL, TX_SEND_ATTEMPTS_TOTAL, TX_SEND_JITO_TOTAL, TX_SEND_RPC_TOTAL,
    TX_SEND_SUCCESS_TOTAL, TX_SEND_TPU_TOTAL,
};
use ironcrab::nats::{
    config_consumer_config, config_subject, slave_consumer_config, NatsClient, NatsConfig,
    CONFIG_STREAM_NAME, STREAM_NAME, TOPIC_CONTROL_REQUESTS, TOPIC_DECISION_RECORDS,
    TOPIC_EXECUTION_RESULTS, TOPIC_PRIORITY_FEE_SAMPLES, TOPIC_TRADE_INTENTS,
};
use ironcrab::solana::cross_dex_handler::CrossDexHandler;
use ironcrab::solana::dex::meteora_dlmm::MeteoraDlmm;
use ironcrab::solana::dex::orca::Orca;
use ironcrab::solana::dex::pumpfun::{BondingCurveState, PumpFunDex};
use ironcrab::solana::dex::pumpfun_amm::PumpFunAmmDex;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::dex::router::Router;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::dex_parser::SOL_MINT;
use ironcrab::solana::jito::{JitoClient, JitoRegion};
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::token_utils::get_token_decimals_or_default;
use ironcrab::solana::tx_sender::TxSender;
use ironcrab::storage::{
    locks::{LockHolder, LockManager, LockResult, ResourceType},
    JsonlWriter, JsonlWriterConfig,
};
use ironcrab::wallet::Treasury;
use spl_token::instruction as spl_ix;
use spl_token::solana_program::program_pack::Pack;
use spl_token::solana_program::pubkey::Pubkey as SplProgPubkey;
use spl_token_2022::instruction as spl22_ix;
use spl_token_2022::{
    extension::StateWithExtensions as Spl22StateWithExtensions, state::Account as Spl22TokenAccount,
};
use std::sync::atomic::AtomicBool;

fn extract_owner_mint_delta_raw(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    owner: &Pubkey,
    mint: &str,
) -> Option<(u8, i128)> {
    let meta = tx.transaction.meta.as_ref()?;

    let pre_balances_opt =
        Option::<Vec<UiTransactionTokenBalance>>::from(meta.pre_token_balances.clone());
    let post_balances_opt =
        Option::<Vec<UiTransactionTokenBalance>>::from(meta.post_token_balances.clone());
    let (pre_balances, post_balances) = (pre_balances_opt?, post_balances_opt?);

    let owner_str = owner.to_string();

    let mut pre_sum: u128 = 0;
    let mut post_sum: u128 = 0;
    let mut decimals_opt: Option<u8> = None;

    for b in pre_balances.iter() {
        let b_owner = Option::<String>::from(b.owner.clone());
        if b.mint == mint && b_owner.as_deref() == Some(&owner_str) {
            decimals_opt = Some(b.ui_token_amount.decimals);
            if let Ok(v) = u128::from_str(&b.ui_token_amount.amount) {
                pre_sum = pre_sum.saturating_add(v);
            }
        }
    }

    for b in post_balances.iter() {
        let b_owner = Option::<String>::from(b.owner.clone());
        if b.mint == mint && b_owner.as_deref() == Some(&owner_str) {
            decimals_opt = Some(b.ui_token_amount.decimals);
            if let Ok(v) = u128::from_str(&b.ui_token_amount.amount) {
                post_sum = post_sum.saturating_add(v);
            }
        }
    }

    let decimals = decimals_opt?;
    let delta = post_sum as i128 - pre_sum as i128;
    Some((decimals, delta))
}

fn find_message_account_index(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    pubkey: &Pubkey,
) -> Option<usize> {
    let needle = pubkey.to_string();

    // We intentionally use a serde_json traversal here to avoid tight coupling to
    // `UiMessage` / `UiParsedMessage` struct variants.
    let v = serde_json::to_value(&tx.transaction.transaction).ok()?;
    let account_keys = v.get("message")?.get("accountKeys")?.as_array()?;
    for (i, ak) in account_keys.iter().enumerate() {
        if let Some(s) = ak.as_str() {
            if s == needle {
                return Some(i);
            }
        } else if let Some(pk) = ak.get("pubkey").and_then(|x| x.as_str()) {
            if pk == needle {
                return Some(i);
            }
        }
    }
    None
}

fn compute_wallet_lamport_delta_best_effort(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    wallet: &Pubkey,
) -> Option<(i128, u64, bool)> {
    let meta = tx.transaction.meta.as_ref()?;
    let wallet_index = find_message_account_index(tx, wallet)?;

    let pre = *meta.pre_balances.get(wallet_index)?;
    let post = *meta.post_balances.get(wallet_index)?;
    let delta = post as i128 - pre as i128;

    // Heuristic: if the tx funds a brand new account or zeroes an account in the message,
    // payer lamport delta is likely polluted by rent / account lifecycle noise.
    let mut has_account_lifecycle_noise = false;
    for (i, (pre_i, post_i)) in meta
        .pre_balances
        .iter()
        .copied()
        .zip(meta.post_balances.iter().copied())
        .enumerate()
    {
        if i == wallet_index {
            continue;
        }
        if pre_i == 0 && post_i > 0 {
            has_account_lifecycle_noise = true;
            break;
        }
        if pre_i > 0 && post_i == 0 {
            has_account_lifecycle_noise = true;
            break;
        }
    }

    Some((delta, meta.fee, has_account_lifecycle_noise))
}

async fn compute_intent_fills_best_effort(
    ctx: &ExecutionContext,
    wallet: Pubkey,
    signature: &Signature,
    intent: &TradeIntent,
) -> (
    Option<ExplicitAmount>,
    Option<ExplicitAmount>,
    FillStatus,
    Option<FillUnavailableReason>,
    Option<i128>, // SOL delta in lamports
) {
    let cfg = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::JsonParsed),
        commitment: Some(solana_commitment_config::CommitmentConfig {
            commitment: CommitmentLevel::Confirmed,
        }),
        max_supported_transaction_version: Some(0),
    };

    let tx = match ctx
        .rpc
        .get_transaction_with_config_retry(signature, cfg)
        .await
    {
        Ok(tx) => tx,
        Err(e) => {
            debug!(sig = %signature, error = %e, "Failed to fetch tx meta for fills (best-effort)");
            return (
                None,
                None,
                FillStatus::Unavailable,
                Some(FillUnavailableReason::RpcTxFetchFailed),
                None,
            );
        }
    };

    if tx.transaction.meta.is_none() {
        return (
            None,
            None,
            FillStatus::Unavailable,
            Some(FillUnavailableReason::TxMetaMissing),
            None,
        );
    }

    let input_mint = intent.resources.input_mint.as_str();
    let output_mint = intent.resources.output_mint.as_str();

    // ARBITRAGE DETECTION: input_mint == output_mint == WSOL means it's an arb cycle
    let is_arb_cycle = input_mint == output_mint && input_mint == SOL_MINT;

    // Get native SOL (lamport) delta for wallet - used for fees and native SOL tracking
    let mut lamport_reason: Option<FillUnavailableReason> = None;
    let (payer_delta_lamports, fee_lamports, lamport_noise) =
        match compute_wallet_lamport_delta_best_effort(&tx, &wallet) {
            Some((d, f, noise)) => {
                if noise {
                    lamport_reason =
                        Some(FillUnavailableReason::LamportDeltaGatedAccountLifecycleNoise);
                }
                (d, f, noise)
            }
            None => {
                lamport_reason = Some(FillUnavailableReason::WalletAccountIndexMissing);
                (0, 0, true)
            }
        };

    // ============ ARBITRAGE CYCLE HANDLING ============
    // For arb (WSOL → ... → WSOL), we need to track:
    // - fill_in: Total WSOL spent in first swap leg (pre_balance - intermediate minimum)
    // - fill_out: Total WSOL received in last swap leg (final_balance after sell)
    // - wallet_sol_delta: Net PnL = fill_out - fill_in - fees (can be negative = loss)
    if is_arb_cycle {
        // Get WSOL token balance delta (this is the net WSOL change, i.e. PnL before native fees)
        let wsol_token_delta = extract_owner_mint_delta_raw(&tx, &wallet, SOL_MINT);

        if let Some((decimals, wsol_delta)) = wsol_token_delta {
            // For arbitrage:
            // - Negative delta means we lost WSOL (unprofitable arb or fees)
            // - Positive delta means we gained WSOL (profitable arb)
            //
            // We need to reconstruct fill_in and fill_out from the TX.
            // The wsol_delta alone is the net, but for display we want:
            // - fill_in: amount used in buy leg
            // - fill_out: amount received from sell leg
            //
            // Best approach: parse all WSOL transfers to/from wallet in the TX
            let (arb_fill_in, arb_fill_out) =
                extract_arb_wsol_flows(&tx, &wallet).unwrap_or((None, None));

            // If we couldn't parse individual legs, approximate from the input amount
            let fill_in = arb_fill_in.or_else(|| {
                // Fallback: use intent's required_capital as fill_in (input amount)
                Some(intent.required_capital.clone())
            });

            // fill_out = fill_in + wsol_delta (since delta = out - in)
            let fill_out = arb_fill_out.or_else(|| {
                fill_in.as_ref().map(|fi| {
                    let in_raw = fi.raw as i128;
                    let out_raw = (in_raw + wsol_delta).max(0) as u64;
                    ExplicitAmount::new(out_raw, decimals)
                })
            });

            // wallet_sol_delta: Combine WSOL token delta with native SOL delta (for Jito tip etc.)
            // Total PnL = WSOL delta + native SOL delta
            let total_sol_delta = if !lamport_noise {
                wsol_delta + payer_delta_lamports
            } else {
                wsol_delta // Native delta unavailable, use WSOL delta only
            };

            return (
                fill_in,
                fill_out,
                FillStatus::Complete,
                None,
                Some(total_sol_delta),
            );
        }

        // Fallback if WSOL token balance not found - use native lamport delta
        if !lamport_noise {
            // For arb without WSOL tracking, native delta is our best approximation
            let fill_in = Some(intent.required_capital.clone());
            let fill_out = {
                let in_raw = intent.required_capital.raw as i128;
                let out_raw = (in_raw + payer_delta_lamports).max(0) as u64;
                Some(ExplicitAmount::new(out_raw, 9))
            };

            return (
                fill_in,
                fill_out,
                FillStatus::Partial,
                Some(FillUnavailableReason::TokenBalanceDeltaMissing),
                Some(payer_delta_lamports),
            );
        }
    }

    // ============ REGULAR BUY/SELL HANDLING ============
    // Primary path: use token-balance deltas (works for most SPL tokens, including WSOL if used).
    let input_token_delta = extract_owner_mint_delta_raw(&tx, &wallet, input_mint);
    let output_token_delta = extract_owner_mint_delta_raw(&tx, &wallet, output_mint);

    let fill_in = if let Some((decimals, delta)) = input_token_delta {
        if delta < 0 {
            Some(ExplicitAmount::new((-delta) as u64, decimals))
        } else {
            None
        }
    } else if input_mint == SOL_MINT && !lamport_noise && payer_delta_lamports < 0 {
        // Approximate: amount spent excluding network fee.
        let spent_total = (-payer_delta_lamports) as u64;
        let spent_ex_fee = spent_total.saturating_sub(fee_lamports);
        if spent_ex_fee > 0 {
            Some(ExplicitAmount::new(spent_ex_fee, 9))
        } else {
            None
        }
    } else if input_mint == SOL_MINT && lamport_noise {
        // BUY with account lifecycle noise (new ATA created):
        // Use intent's required_capital as fill_in (this is the SOL amount we intended to spend).
        // This is accurate for BUYs since the intent specifies the exact SOL input amount.
        Some(intent.required_capital.clone())
    } else {
        None
    };

    let fill_out = if let Some((decimals, delta)) = output_token_delta {
        if delta > 0 {
            Some(ExplicitAmount::new(delta as u64, decimals))
        } else {
            None
        }
    } else if output_mint == SOL_MINT && !lamport_noise && payer_delta_lamports > 0 {
        // Approximate: amount received before fees (add back network fee).
        let received_total = payer_delta_lamports as u64;
        let received_plus_fee = received_total.saturating_add(fee_lamports);
        if received_plus_fee > 0 {
            Some(ExplicitAmount::new(received_plus_fee, 9))
        } else {
            None
        }
    } else {
        None
    };

    let fill_status = match (fill_in.is_some(), fill_out.is_some()) {
        (true, true) => FillStatus::Complete,
        (true, false) | (false, true) => FillStatus::Partial,
        (false, false) => FillStatus::Unavailable,
    };

    let sol_leg_missing = (input_mint == SOL_MINT && fill_in.is_none())
        || (output_mint == SOL_MINT && fill_out.is_none());

    let fill_unavailable_reason = if fill_status == FillStatus::Complete {
        None
    } else if (input_mint == SOL_MINT || output_mint == SOL_MINT) && sol_leg_missing {
        lamport_reason.or(Some(FillUnavailableReason::TokenBalanceDeltaMissing))
    } else {
        Some(FillUnavailableReason::TokenBalanceDeltaMissing)
    };

    // Return wallet SOL delta (5th tuple element) unless gated by noise
    let wallet_sol_delta = if !lamport_noise {
        Some(payer_delta_lamports)
    } else {
        None
    };

    (
        fill_in,
        fill_out,
        fill_status,
        fill_unavailable_reason,
        wallet_sol_delta,
    )
}

/// Extract WSOL flows from arbitrage TX: (amount_spent, amount_received)
/// Parses token balance changes to find the actual WSOL in/out for each leg.
fn extract_arb_wsol_flows(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    wallet: &Pubkey,
) -> Option<(Option<ExplicitAmount>, Option<ExplicitAmount>)> {
    let meta = tx.transaction.meta.as_ref()?;

    let pre_balances_opt =
        Option::<Vec<UiTransactionTokenBalance>>::from(meta.pre_token_balances.clone());
    let post_balances_opt =
        Option::<Vec<UiTransactionTokenBalance>>::from(meta.post_token_balances.clone());
    let (pre_balances, post_balances) = (pre_balances_opt?, post_balances_opt?);

    let wallet_str = wallet.to_string();

    // Find WSOL accounts owned by wallet
    let mut wsol_pre: u64 = 0;
    let mut wsol_post: u64 = 0;

    for b in pre_balances.iter() {
        let b_owner = Option::<String>::from(b.owner.clone());
        if b.mint == SOL_MINT && b_owner.as_deref() == Some(&wallet_str) {
            if let Ok(v) = u64::from_str(&b.ui_token_amount.amount) {
                wsol_pre = wsol_pre.saturating_add(v);
            }
        }
    }

    for b in post_balances.iter() {
        let b_owner = Option::<String>::from(b.owner.clone());
        if b.mint == SOL_MINT && b_owner.as_deref() == Some(&wallet_str) {
            if let Ok(v) = u64::from_str(&b.ui_token_amount.amount) {
                wsol_post = wsol_post.saturating_add(v);
            }
        }
    }

    // For arb: fill_in is the max WSOL we had, fill_out is what we ended with
    // In a typical arb: we start with X WSOL, buy tokens (WSOL drops to 0), sell tokens (WSOL returns)
    // The issue is we only see pre and post, not the intermediate.
    //
    // Better approach: use the intent's input amount as fill_in (what we intended to spend)
    // and wsol_post as fill_out (what we actually ended up with)
    //
    // For now, return (pre, post) and let caller compute delta
    if wsol_pre > 0 || wsol_post > 0 {
        let fill_in = if wsol_pre > wsol_post {
            // We spent more than we received = lost WSOL
            Some(ExplicitAmount::new(wsol_pre, 9))
        } else {
            // We received more than we started = gained WSOL (unusual for arb input)
            Some(ExplicitAmount::new(wsol_pre, 9))
        };

        let fill_out = Some(ExplicitAmount::new(wsol_post, 9));

        Some((fill_in, fill_out))
    } else {
        None
    }
}

/// DEPRECATED: This function is no longer used in the multi-process architecture.
/// Wallet scanning is now handled by market-data (Data Plane), not execution-engine.
/// Kept for potential debugging/manual inspection only.
#[allow(dead_code)]
async fn discover_wallet_open_positions(rpc: &SolanaRpc, owner: Pubkey) -> anyhow::Result<usize> {
    let token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")?;
    let token_2022_program_id = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;
    let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")?;

    let mut token_accounts = rpc
        .rpc
        .get_token_accounts_by_owner(&owner, TokenAccountsFilter::ProgramId(token_program_id))
        .await?;

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

    // Count non-zero token accounts excluding SOL/WSOL mint.
    // NOTE: This is a best-effort “wallet holdings” view; it does not reconstruct cost basis.
    let mut seen_accounts = HashSet::new();
    let mut count = 0usize;

    for ta in token_accounts {
        // Defensive: avoid double-counting if RPC returns duplicates.
        if !seen_accounts.insert(ta.pubkey.clone()) {
            continue;
        }

        let parsed = match ta.account.data {
            UiAccountData::Json(parsed) => parsed,
            UiAccountData::Binary(data, enc) => {
                let bytes = match enc {
                    UiAccountEncoding::Base58 => bs58::decode(data).into_vec().ok(),
                    UiAccountEncoding::Base64 => {
                        base64::engine::general_purpose::STANDARD.decode(data).ok()
                    }
                    _ => None,
                };
                let Some(bytes) = bytes else { continue };
                if bytes.len() < 72 {
                    continue;
                }
                // Check frozen (SPL token account state at byte 108)
                if bytes.len() >= 109 && bytes[108] == 2 {
                    continue;
                }
                let mint_bytes: [u8; 32] = match bytes.get(0..32).and_then(|s| s.try_into().ok()) {
                    Some(m) => m,
                    None => continue,
                };
                let mint = Pubkey::new_from_array(mint_bytes);
                let amount_bytes: [u8; 8] = match bytes.get(64..72).and_then(|s| s.try_into().ok())
                {
                    Some(a) => a,
                    None => continue,
                };
                let amount = u64::from_le_bytes(amount_bytes);
                if mint == sol_mint || amount == 0 {
                    continue;
                }
                count += 1;
                continue;
            }
            UiAccountData::LegacyBinary(data) => {
                let bytes = bs58::decode(data).into_vec().ok();
                let Some(bytes) = bytes else { continue };
                if bytes.len() < 72 {
                    continue;
                }
                if bytes.len() >= 109 && bytes[108] == 2 {
                    continue;
                }
                let mint_bytes: [u8; 32] = match bytes.get(0..32).and_then(|s| s.try_into().ok()) {
                    Some(m) => m,
                    None => continue,
                };
                let mint = Pubkey::new_from_array(mint_bytes);
                let amount_bytes: [u8; 8] = match bytes.get(64..72).and_then(|s| s.try_into().ok())
                {
                    Some(a) => a,
                    None => continue,
                };
                let amount = u64::from_le_bytes(amount_bytes);
                if mint == sol_mint || amount == 0 {
                    continue;
                }
                count += 1;
                continue;
            }
        };

        // JsonParsed
        let serde_json::Value::Object(root) = parsed.parsed else {
            continue;
        };
        let Some(info) = root.get("info") else {
            continue;
        };

        let is_frozen = info
            .get("state")
            .and_then(|s| s.as_str())
            .map(|s| s.eq_ignore_ascii_case("frozen"))
            .unwrap_or(false);
        if is_frozen {
            continue;
        }

        let mint_str = match info.get("mint").and_then(|v| v.as_str()) {
            Some(m) => m,
            None => continue,
        };
        let mint = match Pubkey::from_str(mint_str) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if mint == sol_mint {
            continue;
        }

        let amount_str = info
            .get("tokenAmount")
            .and_then(|v| v.get("amount"))
            .and_then(|v| v.as_str())
            .unwrap_or("0");
        let amount = amount_str.parse::<u64>().unwrap_or(0);
        if amount == 0 {
            continue;
        }

        count += 1;
    }

    Ok(count)
}

/// NATS topic for config reload commands from control-plane
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";

// P1 Crash Isolation: Systemd Watchdog support (Linux only)
#[cfg(unix)]
use sd_notify::NotifyState;

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser, Debug)]
#[command(name = "execution-engine")]
#[command(about = "IronCrab Execution Plane – Single Signer, Tx Plan/Sim/Send")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// NATS server URL
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Solana RPC URL
    #[arg(long, env = "RPC_URL", default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    /// Prometheus metrics port
    #[arg(long, default_value = "9804")]
    metrics_port: u16,

    /// Log directory override
    #[arg(long, env = "IRONCRAB_LOG_DIR")]
    log_dir: Option<PathBuf>,

    /// Disable actual transaction sending (simulation only)
    #[arg(long)]
    simulate_only: bool,

    /// Dry run: never send on-chain transactions (may still do read-only RPC for checks)
    #[arg(long)]
    dry_run: bool,

    /// Initial SOL balance for lock manager (lamports)
    #[arg(long, default_value = "1000000000")]
    initial_sol_lamports: u64,
}

/// Execution engine configuration
///
/// All risk limits are documented here (DoD J) P0: No hidden defaults).
/// These values are checked before every trade execution.
#[derive(Debug, Clone)]
struct ExecutionConfig {
    // === Risk Invariants (DoD J) P0) ===
    /// Maximum single position size (lamports). Default: 0.5 SOL
    /// Rejects any intent with required_capital > this value.
    max_position_size_lamports: u64,

    /// Maximum daily loss (lamports) before kill switch. Default: 5 SOL
    /// Tracks cumulative losses within a calendar day (UTC).
    daily_loss_limit_lamports: u64,

    /// Maximum concurrent open positions. Default: 5
    /// Rejects new intents if this limit is reached.
    max_open_positions: usize,

    /// Maximum allowed slippage (basis points). Default: 500 (5%)
    /// Rejects any intent with max_slippage_bps > this value.
    max_slippage_bps: u32,

    // === Operational Config ===
    /// Simulation timeout (ms)
    simulation_timeout_ms: u64,

    /// Confirmation timeout (ms) for RPC send path
    confirmation_timeout_ms: u64,

    /// RPC sendTransaction: skip preflight (safe default when simulate-gated)
    send_skip_preflight: bool,

    /// RPC sendTransaction: preflight commitment ("processed"|"confirmed"|"finalized"); None uses RPC default
    send_preflight_commitment: Option<String>,

    /// Whether to actually send transactions
    send_enabled: bool,

    // === P1: Jito Bundle Config ===
    /// Enable Jito bundle submission for atomic execution
    jito_enabled: bool,

    /// Default tip amount for Jito bundles (lamports)
    jito_tip_lamports: u64,

    /// Jito block engine region (frankfurt, amsterdam, ny, tokyo, slc)
    jito_region: String,

    /// Timeout for bundle confirmation (seconds)
    jito_timeout_secs: u64,

    // === P1: Fee/Compute Policies ===
    /// Centralized fee policy (engine owns compute budget and priority fees)
    fee_policy: FeePolicy,
    /// Optional liquidation-specific fee overrides (kill switch / liquidation sells)
    liquidation_priority_fee_micro_lamports: Option<u64>,
    liquidation_max_priority_fee_micro_lamports: Option<u64>,
    liquidation_max_tx_cost_lamports: Option<u64>,

    // === P1: Fairness/Starvation Policy ===
    /// Fairness policy to prevent strategy starvation
    #[allow(dead_code)]
    fairness_policy: FairnessPolicy,

    // === WSOL Manager Config (hot-reloadable) ===
    wsol_enabled: bool,
    wsol_min_wsol_sol: f64,
    wsol_target_wsol_sol: f64,
    wsol_max_wsol_sol: f64,
    wsol_min_native_sol: f64,
    wsol_cooldown_secs: u64,
    wsol_dry_run: bool,

    // === Account Janitor Config (hot-reloadable) ===
    janitor_enabled: bool,
    janitor_close_ata_interval_secs: u64,
    janitor_close_ata_min_age_secs: u64,
    janitor_close_ata_max_per_run: usize,
    janitor_merge_dust_enabled: bool,
    janitor_merge_dust_interval_secs: u64,
    janitor_merge_dust_max_per_run: usize,
    janitor_swap_dust_enabled: bool,
    janitor_swap_dust_interval_secs: u64,
    janitor_swap_dust_min_value_sol: f64,
    janitor_swap_dust_max_slippage_bps: u32,
    janitor_swap_dust_max_per_run: usize,
    janitor_dry_run: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            // Risk Invariants - conservative defaults for safety
            max_position_size_lamports: 500_000_000, // 0.5 SOL max per trade
            daily_loss_limit_lamports: 5_000_000_000, // 5 SOL daily loss limit
            max_open_positions: 5,                   // max 5 concurrent positions
            max_slippage_bps: 500,                   // max 5% slippage allowed
            // Operational
            simulation_timeout_ms: 2000,
            confirmation_timeout_ms: 30_000,
            send_skip_preflight: true,
            send_preflight_commitment: None,
            send_enabled: false, // Default: simulate only
            // P1: Jito Bundle defaults
            jito_enabled: false,
            jito_tip_lamports: 10_000, // 0.00001 SOL default tip
            jito_region: "frankfurt".to_string(),
            jito_timeout_secs: 30,
            // P1: Fee/Compute Policy
            fee_policy: FeePolicy::default(),
            liquidation_priority_fee_micro_lamports: None,
            liquidation_max_priority_fee_micro_lamports: None,
            liquidation_max_tx_cost_lamports: None,
            // P1: Fairness Policy
            fairness_policy: FairnessPolicy::default(),
            // WSOL Manager defaults
            wsol_enabled: true,
            wsol_min_wsol_sol: 0.5,
            wsol_target_wsol_sol: 1.0,
            wsol_max_wsol_sol: 2.0,
            wsol_min_native_sol: 0.1,
            wsol_cooldown_secs: 30,
            wsol_dry_run: false,
            // Account Janitor defaults
            janitor_enabled: false,
            janitor_close_ata_interval_secs: 3600,
            janitor_close_ata_min_age_secs: 86400,
            janitor_close_ata_max_per_run: 10,
            janitor_merge_dust_enabled: false,
            janitor_merge_dust_interval_secs: 300,
            janitor_merge_dust_max_per_run: 5,
            janitor_swap_dust_enabled: false,
            janitor_swap_dust_interval_secs: 86400,
            janitor_swap_dust_min_value_sol: 0.001,
            janitor_swap_dust_max_slippage_bps: 500,
            janitor_swap_dust_max_per_run: 5,
            janitor_dry_run: false,
        }
    }
}

impl ExecutionConfig {
    /// Returns a snapshot ID for this config (for Decision Record correlation)
    fn snapshot_id(&self) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        format!("{:?}", self).hash(&mut hasher);
        format!("cfg-{:016x}", hasher.finish())
    }
}

// ============================================================================
// P1: State Persistence (DoD K) - State survives restarts
// ============================================================================

/// Persistent state snapshot for crash recovery
///
/// Saved on graceful shutdown and periodic intervals.
/// Loaded on startup to restore state.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateSnapshot {
    /// Version for forward compatibility
    version: u32,
    /// UTC date for daily tracking
    day: String,
    /// Cumulative daily loss (lamports, positive = loss)
    daily_loss_lamports: i64,
    /// Current open positions count
    open_positions: usize,
    /// Decision counter (for generating unique IDs)
    decision_counter: u64,
    /// Execution counter
    execution_counter: u64,
    /// Processed intent IDs (idempotency store)
    processed_intents: Vec<String>,
    /// Timestamp when snapshot was created
    saved_at: String,
    /// Run ID that created this snapshot
    run_id: String,

    /// Operational kill switch state. When true: reject new BUY intents.
    ///
    /// `serde(default)` keeps backward compatibility with older snapshots.
    #[serde(default)]
    kill_switch_active: bool,
}

impl StateSnapshot {
    const CURRENT_VERSION: u32 = 1;
    const SNAPSHOT_FILE: &'static str = "execution_state.json";

    /// Create a new snapshot from current state
    fn from_context(ctx: &ExecutionContext) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            day: ctx.current_day.read().to_string(),
            daily_loss_lamports: ctx
                .daily_loss_lamports
                .load(std::sync::atomic::Ordering::Relaxed),
            open_positions: ctx
                .open_positions
                .load(std::sync::atomic::Ordering::Relaxed),
            decision_counter: ctx
                .decision_counter
                .load(std::sync::atomic::Ordering::Relaxed),
            execution_counter: ctx
                .execution_counter
                .load(std::sync::atomic::Ordering::Relaxed),
            processed_intents: ctx.lock_manager.get_processed_intents(),
            saved_at: chrono::Utc::now().to_rfc3339(),
            run_id: ctx.run_id.clone(),
            kill_switch_active: ctx
                .kill_switch_active
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Save snapshot to disk
    fn save(&self, log_dir: &Path) -> Result<()> {
        let path = log_dir.join(Self::SNAPSHOT_FILE);
        std::fs::create_dir_all(log_dir)?;
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, json)?;
        info!(path = %path.display(), "State snapshot saved");
        Ok(())
    }

    /// Load snapshot from disk (returns None if not found or invalid)
    fn load(log_dir: &Path) -> Option<Self> {
        let path = log_dir.join(Self::SNAPSHOT_FILE);
        if !path.exists() {
            info!(path = %path.display(), "No state snapshot found, starting fresh");
            return None;
        }

        match std::fs::read_to_string(&path) {
            Ok(json) => match serde_json::from_str::<StateSnapshot>(&json) {
                Ok(snapshot) => {
                    if snapshot.version != Self::CURRENT_VERSION {
                        warn!(
                            found_version = snapshot.version,
                            expected_version = Self::CURRENT_VERSION,
                            "State snapshot version mismatch, starting fresh"
                        );
                        return None;
                    }
                    info!(
                        path = %path.display(),
                        saved_at = %snapshot.saved_at,
                        prev_run_id = %snapshot.run_id,
                        processed_intents = snapshot.processed_intents.len(),
                        kill_switch_active = snapshot.kill_switch_active,
                        "Loaded state snapshot"
                    );
                    Some(snapshot)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to parse state snapshot, starting fresh");
                    None
                }
            },
            Err(e) => {
                warn!(error = %e, "Failed to read state snapshot, starting fresh");
                None
            }
        }
    }

    /// Check if the snapshot is from the same day
    fn is_same_day(&self) -> bool {
        let today = chrono::Utc::now().date_naive().to_string();
        self.day == today
    }
}

#[derive(Debug, Clone)]
struct RouteCandidate {
    dex: String,
    amount_out: u64,
    pool_id: String,
    accounts: Vec<String>,
    creator: Option<String>,
}

fn select_best_route(candidates: Vec<RouteCandidate>) -> Option<RouteCandidate> {
    let mut best: Option<RouteCandidate> = None;
    for candidate in candidates {
        let replace = best
            .as_ref()
            .map(|b| candidate.amount_out > b.amount_out)
            .unwrap_or(true);
        if replace {
            best = Some(candidate);
        }
    }
    best
}

/// Runtime context for execution-engine
struct ExecutionContext {
    run_id: String,
    rpc_url: String,
    helius_rpc_url: Option<String>,
    wallet_pubkey: Option<Pubkey>,
    /// The ONLY signer (Single-Signer rule). None means keyless mode.
    treasury: Option<Treasury>,
    /// Hot-reloadable configuration (RwLock for runtime updates via NATS)
    config: parking_lot::RwLock<ExecutionConfig>,
    config_snapshot_id: parking_lot::RwLock<String>,
    nats: Option<NatsClient>,
    decision_writer: JsonlWriter,
    execution_writer: JsonlWriter,
    burn_writer: JsonlWriter,
    lock_manager: LockManager,
    log_base: PathBuf, // P1: For state persistence
    decision_counter: std::sync::atomic::AtomicU64,
    execution_counter: std::sync::atomic::AtomicU64,

    // === Risk Tracking (DoD J) P0) ===
    /// Current day (UTC) for daily loss tracking
    current_day: parking_lot::RwLock<chrono::NaiveDate>,
    /// Cumulative loss today (lamports, positive = loss)
    daily_loss_lamports: std::sync::atomic::AtomicI64,
    /// Currently open positions count
    open_positions: std::sync::atomic::AtomicUsize,

    // === Operational Kill Switch ===
    /// When active: reject new BUY intents.
    kill_switch_active: AtomicBool,
    /// Prevent concurrent liquidation jobs.
    liquidation_in_progress: AtomicBool,
    /// Last kill switch context from control-plane.
    kill_switch_context: parking_lot::RwLock<Option<KillSwitchContext>>,

    /// Prevent concurrent manual burn jobs.
    burn_in_progress: AtomicBool,

    // === P1: Jito Bundle Support ===
    /// Jito client for atomic bundle execution (None if disabled)
    jito_client: Option<JitoClient>,
    /// Bundle submissions counter
    #[allow(dead_code)]
    bundles_submitted: std::sync::atomic::AtomicU64,
    /// Bundle confirmations counter
    #[allow(dead_code)]
    bundles_confirmed: std::sync::atomic::AtomicU64,

    // === Cross-DEX Arbitrage Handler ===
    /// Handler for cross-DEX arb intents (optional, requires RPC)
    cross_dex_handler: Option<Arc<CrossDexHandler>>,
    /// RPC wrapper for read-only queries and (future) sim/send
    rpc: Arc<SolanaRpc>,

    // === Address Lookup Table (P0: TX size reduction) ===
    /// Loaded ALT for versioned transactions (reduces TX size by ~60%)
    address_lookup_table: Option<ironcrab::solana::address_lookup_table::LoadedAlt>,

    // === P2: TxSender with TPU/Jito/RPC fallback chain ===
    /// Unified transaction sender with automatic fallback (TPU → Jito → RPC)
    tx_sender: Option<Arc<TxSender>>,

    // === P2: Dynamic Priority Fees (from Geyser via market-data NATS) ===
    /// Latest priority fee percentiles from market-data (None = use static config)
    dynamic_fee_percentiles: parking_lot::RwLock<Option<PriorityFeePercentiles>>,

    // === Option C: Live Pool Cache (P0: fresh quotes, no RPC in hot path) ===
    /// Cache of pool states from Geyser for fresh quote calculation
    live_pool_cache: Option<Arc<ironcrab::execution::live_pool_cache::LivePoolCache>>,

    // Metrics
    intents_received: std::sync::atomic::AtomicU64,
    intents_rejected: std::sync::atomic::AtomicU64,
    sim_failures: std::sync::atomic::AtomicU64,
    #[allow(dead_code)]
    tx_sent: std::sync::atomic::AtomicU64,
    arb_validated: std::sync::atomic::AtomicU64,
    #[allow(dead_code)]
    arb_executed: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
struct BurnOpRecord {
    #[serde(flatten)]
    header: RecordHeader,
    request_id: String,
    wallet: String,
    token_account: String,
    mint: String,
    token_program: String,
    amount_raw: u64,
    close_accounts: bool,
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl ExecutionContext {
    /// Get current config (read lock)
    fn get_config(&self) -> ExecutionConfig {
        self.config.read().clone()
    }

    fn is_kill_switch_active(&self) -> bool {
        self.kill_switch_active.load(Ordering::Relaxed)
    }

    fn set_kill_switch_context(&self, context: Option<KillSwitchContext>) {
        *self.kill_switch_context.write() = context;
    }

    fn get_kill_switch_context(&self) -> Option<KillSwitchContext> {
        self.kill_switch_context.read().clone()
    }

    /// Get priority fee for an intent using dynamic percentiles if available, else static config.
    ///
    /// P2: Dynamic Priority Fee Usage
    /// - If market-data is publishing percentiles via NATS, use tier-specific recommended fee
    /// - Otherwise fall back to static config from FeePolicy
    fn get_priority_fee_for_intent(&self, intent: &TradeIntent, fee_policy: &FeePolicy) -> u64 {
        if let Some(ref percentiles) = *self.dynamic_fee_percentiles.read() {
            let dynamic_fee = match intent.tier {
                IntentTier::Tier0 => percentiles.tier0_recommended,
                IntentTier::Tier1 => percentiles.tier1_recommended,
                IntentTier::Arb => percentiles.arb_recommended,
            };
            tracing::debug!(
                intent_id = %intent.intent_id,
                tier = ?intent.tier,
                dynamic_fee_micro_lamports = dynamic_fee,
                p50 = percentiles.p50,
                p90 = percentiles.p90,
                sample_count = percentiles.sample_count,
                "Using DYNAMIC priority fee from Geyser percentiles"
            );
            dynamic_fee
        } else {
            let static_fee = fee_policy.priority_fee_for_intent(intent);
            tracing::debug!(
                intent_id = %intent.intent_id,
                tier = ?intent.tier,
                static_fee_micro_lamports = static_fee,
                "Using STATIC priority fee (no dynamic percentiles available)"
            );
            static_fee
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

    fn prog_ix_to_sdk(
        ix: spl_token::solana_program::instruction::Instruction,
    ) -> solana_sdk::instruction::Instruction {
        solana_sdk::instruction::Instruction {
            program_id: Pubkey::new_from_array(ix.program_id.to_bytes()),
            accounts: ix
                .accounts
                .into_iter()
                .map(|a| solana_sdk::instruction::AccountMeta {
                    pubkey: Pubkey::new_from_array(a.pubkey.to_bytes()),
                    is_signer: a.is_signer,
                    is_writable: a.is_writable,
                })
                .collect(),
            data: ix.data,
        }
    }

    fn ata_for_owner_mint(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
        let owner_spl = Self::sdk_to_spl(owner);
        let mint_spl = Self::sdk_to_spl(mint);
        let token_prog_spl = Self::sdk_to_spl(token_program);
        let ata_spl = spl_associated_token_account::get_associated_token_address_with_program_id(
            &owner_spl,
            &mint_spl,
            &token_prog_spl,
        );
        Self::spl_to_sdk(&ata_spl)
    }

    /// Get token program for a mint - GEYSER-FIRST, NO RPC!
    /// Uses LivePoolCache which is populated by Geyser mint account subscriptions.
    /// For pump.fun tokens, defaults to SPL Token (they never use Token-2022).
    fn token_program_for_mint_cached(
        cache: Option<&ironcrab::execution::live_pool_cache::LivePoolCache>,
        mint: &Pubkey,
        dex_hint: Option<&str>,
    ) -> Pubkey {
        let spl = Pubkey::new_from_array(spl_token::id().to_bytes());
        let _spl22 = Pubkey::new_from_array(spl_token_2022::id().to_bytes());

        // Try cache first
        if let Some(c) = cache {
            if let Some(prog) = c.get_mint_program(mint) {
                return prog;
            }
        }

        // Fallback: pump.fun/pumpfun/pump_amm always use SPL Token
        if let Some(dex) = dex_hint {
            let dex_lower = dex.to_lowercase();
            if dex_lower.contains("pump") || dex_lower == "pumpfun" || dex_lower == "pump_amm" {
                return spl;
            }
        }

        // Default to SPL Token (most common case)
        // NOTE: For Token-2022 tokens without cache hit, this may be wrong
        // but those are rare and the TX will fail simulation anyway
        spl
    }

    /// Legacy RPC-based token program lookup - DEPRECATED, only for non-hot-path code
    #[allow(dead_code)]
    async fn token_program_for_mint_rpc(rpc: &SolanaRpc, mint: &Pubkey) -> anyhow::Result<Pubkey> {
        let acct = rpc.rpc.get_account(mint).await?;
        let owner = acct.owner;

        let spl = Pubkey::new_from_array(spl_token::id().to_bytes());
        let spl22 = Pubkey::new_from_array(spl_token_2022::id().to_bytes());

        if owner == spl {
            Ok(spl)
        } else if owner == spl22 {
            Ok(spl22)
        } else {
            anyhow::bail!(
                "Mint owner is neither spl-token nor spl-token-2022: {}",
                owner
            );
        }
    }

    fn apply_slippage_min_out(quoted_out: u64, slippage_bps: u32) -> u64 {
        let keep_bps = 10_000u64.saturating_sub(slippage_bps as u64);
        ((quoted_out as u128) * (keep_bps as u128) / 10_000u128) as u64
    }

    async fn run_liquidation_job(
        ctx: Arc<ExecutionContext>,
        max_slippage_bps: u32,
        ttl_ms: u64,
        reason: Option<String>,
    ) {
        #[cfg(unix)]
        let mut last_watchdog_ping = std::time::Instant::now();
        #[cfg(unix)]
        let mut maybe_ping_watchdog = || {
            if last_watchdog_ping.elapsed() >= Duration::from_secs(5) {
                let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
                last_watchdog_ping = std::time::Instant::now();
            }
        };

        if ctx
            .liquidation_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            warn!("Liquidation already in progress; ignoring new request");
            return;
        }
        struct LiquidationInProgressGuard {
            ctx: Arc<ExecutionContext>,
        }
        impl Drop for LiquidationInProgressGuard {
            fn drop(&mut self) {
                self.ctx
                    .liquidation_in_progress
                    .store(false, Ordering::SeqCst);
            }
        }
        let _guard = LiquidationInProgressGuard {
            ctx: Arc::clone(&ctx),
        };

        let Some(owner) = ctx.wallet_pubkey else {
            warn!("Liquidation requested but wallet_pubkey is None");
            return;
        };
        if ctx.treasury.is_none() {
            warn!("Liquidation requested but treasury (signer) is None");
            return;
        }

        info!(wallet = %owner, max_slippage_bps, ttl_ms, "Starting liquidation job");
        #[cfg(unix)]
        maybe_ping_watchdog();

        // Initialize DEX connectors for quote discovery.
        // Order priority: Pump.fun bonding curve (known pool) → multi-pool best quote
        let pumpfun = match PumpFunDex::new(Arc::clone(&ctx.rpc)) {
            Ok(p) => Some(p),
            Err(e) => {
                warn!(error = %e, "Failed to init PumpFunDex; continuing with other DEXes");
                None
            }
        };
        let pump_amm = PumpFunAmmDex::new(
            Arc::clone(&ctx.rpc),
            ctx.rpc_url.clone(),
            ctx.helius_rpc_url.clone(),
        );
        let mut meteora = MeteoraDlmm::new(Arc::clone(&ctx.rpc));
        meteora.set_user_authority(owner);
        let raydium = Raydium::new(Arc::clone(&ctx.rpc));
        let orca = Orca::new(Arc::clone(&ctx.rpc));
        orca.set_user_authority(owner);

        if let Err(e) = raydium.refresh_pools().await {
            warn!(error = %e, "Raydium refresh_pools failed; liquidation may miss routes");
        }
        if let Err(e) = orca.refresh_pools().await {
            warn!(error = %e, "Orca refresh_pools failed; liquidation may miss routes");
        }
        // Meteora + Orca: inject cached pools from Geyser LivePoolCache (NO RPC needed!)
        // The pools are already cached from market-data via Geyser subscription.
        if let Some(ref cache) = ctx.live_pool_cache {
            let mut meteora_count = 0;
            let mut orca_count = 0;
            for (pool_addr, state) in cache.iter() {
                match state {
                    CachedPoolState::Meteora(ref ms) => {
                        // Only inject if we have real Geyser data (active_id != 0)
                        // Default active_id=0 means no real data, would cause wrong bin array derivation
                        if ms.active_id != 0
                            && meteora.inject_cached_meteora_state(&pool_addr, ms).is_ok()
                        {
                            meteora_count += 1;
                        }
                    }
                    CachedPoolState::Orca(ref os) => {
                        if orca.inject_cached_orca_state(&pool_addr, os).is_ok() {
                            orca_count += 1;
                        }
                    }
                    _ => {}
                }
            }
            info!(
                meteora_pools = meteora_count,
                orca_pools = orca_count,
                "DEX pools injected from LivePoolCache (GEYSER-FIRST)"
            );
        }
        #[cfg(unix)]
        maybe_ping_watchdog();

        let token_program_id =
            Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let token_2022_program_id =
            Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap();
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();

        let mut token_accounts = match ctx
            .rpc
            .rpc
            .get_token_accounts_by_owner(&owner, TokenAccountsFilter::ProgramId(token_program_id))
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "Failed to list token accounts (spl-token)");
                Vec::new()
            }
        };
        #[cfg(unix)]
        maybe_ping_watchdog();

        if let Ok(mut accounts_2022) = ctx
            .rpc
            .rpc
            .get_token_accounts_by_owner(
                &owner,
                TokenAccountsFilter::ProgramId(token_2022_program_id),
            )
            .await
        {
            token_accounts.append(&mut accounts_2022);
        }
        #[cfg(unix)]
        maybe_ping_watchdog();

        let mut seen_accounts = HashSet::new();
        let mut liquidation_intents: Vec<TradeIntent> = Vec::new();

        for ta in token_accounts {
            #[cfg(unix)]
            maybe_ping_watchdog();

            if !seen_accounts.insert(ta.pubkey.clone()) {
                continue;
            }

            let ta_pubkey = match Pubkey::from_str(&ta.pubkey) {
                Ok(p) => p,
                Err(_) => continue,
            };

            // Extract mint+amount from JsonParsed only (safe/fast for liquidation).
            let parsed = match ta.account.data {
                UiAccountData::Json(parsed) => parsed,
                _ => continue,
            };
            let serde_json::Value::Object(root) = parsed.parsed else {
                continue;
            };
            let info = match root.get("info") {
                Some(v) => v,
                None => continue,
            };
            let mint_str = match info.get("mint").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let amount_str = info
                .get("tokenAmount")
                .and_then(|v| v.get("amount"))
                .and_then(|v| v.as_str())
                .unwrap_or("0");

            let mint = match Pubkey::from_str(mint_str) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if mint == sol_mint {
                continue;
            }
            let amount_in: u64 = match amount_str.parse() {
                Ok(a) => a,
                Err(_) => continue,
            };
            if amount_in == 0 {
                continue;
            }

            // Use the token account's owner (= token program) directly from RPC response
            // This is more reliable than cache lookup for Token-2022 tokens
            let token_program = Pubkey::from_str(&ta.account.owner).unwrap_or_else(|_| {
                // Fallback to cache-based lookup if owner parsing fails
                Self::token_program_for_mint_cached(
                    ctx.live_pool_cache.as_deref(),
                    &mint,
                    None, // No DEX hint in liquidation context
                )
            });
            #[cfg(unix)]
            maybe_ping_watchdog();
            let ata = Self::ata_for_owner_mint(&owner, &mint, &token_program);
            if ta_pubkey != ata {
                continue;
            }

            let decimals = get_token_decimals_or_default(ctx.rpc.as_ref(), &mint).await;
            #[cfg(unix)]
            maybe_ping_watchdog();

            // Build metadata/resources similar to sell-all.
            let mut metadata: HashMap<String, String> = HashMap::new();
            metadata.insert("purpose".to_string(), "liquidation".to_string());
            metadata.insert("kill_switch".to_string(), "true".to_string());
            metadata.insert("mint_decimals".to_string(), decimals.to_string());
            metadata.insert("token_account".to_string(), ta_pubkey.to_string());
            metadata.insert("token_program".to_string(), token_program.to_string());
            if let Some(r) = &reason {
                metadata.insert("kill_reason".to_string(), r.clone());
            }

            let mut resources = TradeResources {
                input_mint: mint.to_string(),
                output_mint: sol_mint.to_string(),
                pools: vec![],
                accounts: vec![ta_pubkey.to_string()],
                token_program: Some(token_program.to_string()),
            };

            let mut min_out_sol: Option<u64> = None;
            let mut quote_attempts: Vec<String> = Vec::new();

            // Prefer Pump.fun if quote exists.
            if min_out_sol.is_none() {
                if let Some(ref pumpfun) = pumpfun {
                    match pumpfun
                        .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), amount_in)
                        .await
                    {
                        Ok(Some(q)) => {
                            #[cfg(unix)]
                            maybe_ping_watchdog();

                            // For Pump.fun, we include the bonding-curve account pubkey as
                            // `resources.pools[0]` to keep tx planning deterministic and to satisfy the
                            // tx_builder invariant that exactly one pool id is provided.
                            if let Some(bc_str) = q.route.first() {
                                resources.pools = vec![bc_str.clone()];

                                // Try to get creator from LivePoolCache first (Geyser-first)
                                let mut _creator_found = false;
                                if let Ok(bc) = Pubkey::from_str(bc_str) {
                                    if let Some(cache) = ctx.live_pool_cache.as_ref() {
                                        if let Some(CachedPoolState::PumpFun(pf)) = cache.get(&bc) {
                                            // PumpFunState doesn't have creator yet - skip for now
                                            // The creator is only needed for metadata, not TX execution
                                            let _ = pf; // suppress unused warning
                                        }
                                    }
                                    // TODO: Remove this RPC fallback once PumpFunState includes creator
                                    // This is liquidation path (not hot-path), so acceptable for now
                                    if !_creator_found {
                                        if let Ok(acct) = ctx.rpc.rpc.get_account(&bc).await {
                                            if let Ok(state) = BondingCurveState::parse(&acct.data)
                                            {
                                                metadata.insert(
                                                    "creator".to_string(),
                                                    state.creator.to_string(),
                                                );
                                                _creator_found = true;
                                            }
                                        }
                                    }
                                }
                            }
                            #[cfg(unix)]
                            maybe_ping_watchdog();

                            if metadata.contains_key("creator") && resources.pools.len() == 1 {
                                metadata.insert(
                                    "sell_routing".to_string(),
                                    "primary".to_string(),
                                );
                                metadata.insert("dex".to_string(), "pumpfun".to_string());
                                min_out_sol = Some(Self::apply_slippage_min_out(
                                    q.amount_out,
                                    max_slippage_bps,
                                ));
                                quote_attempts.push(format!(
                                    "pumpfun=ok amount_out={} route={}",
                                    q.amount_out,
                                    resources
                                        .pools
                                        .first()
                                        .map(|s| s.as_str())
                                        .unwrap_or("<none>")
                                ));
                            } else {
                                quote_attempts.push(format!(
                                    "pumpfun=skip missing_creator_or_pool creator_present={} pools_len={}"
                                    ,
                                    metadata.contains_key("creator"),
                                    resources.pools.len()
                                ));
                            }
                        }
                        Ok(None) => {
                            quote_attempts.push("pumpfun=none".to_string());
                        }
                        Err(e) => {
                            quote_attempts.push(format!("pumpfun=err {e:#}"));
                        }
                    }
                }
            }

            // Fallback to multi-pool routing (PumpSwap + Meteora + Raydium + Orca).
            // Pump.fun bonding curve is handled above as the known pool.
            if min_out_sol.is_none() {
                let mut candidates: Vec<RouteCandidate> = Vec::new();
                let mut record_candidate = |dex: &str,
                                            amount_out: u64,
                                            pool_id: String,
                                            accounts: Vec<String>| {
                    candidates.push(RouteCandidate {
                        dex: dex.to_string(),
                        amount_out,
                        pool_id,
                        accounts,
                        creator: None,
                    });
                };

                // PumpSwap (Pump.fun AMM) with timeout guard.
                let pump_amm_quote = tokio::time::timeout(
                    Duration::from_secs(10),
                    pump_amm.quote_exact_in(&mint.to_string(), &sol_mint.to_string(), amount_in),
                )
                .await;
                match pump_amm_quote {
                    Err(_timeout) => {
                        quote_attempts.push("pump_amm=timeout (10s)".to_string());
                    }
                    Ok(inner) => match inner {
                        Ok(Some(q)) => {
                            #[cfg(unix)]
                            maybe_ping_watchdog();
                            if let Some(pool_id) = q.route.first().cloned() {
                                match pump_amm.pool_accounts_v1_for_base_mint(mint).await {
                                    Ok(Some(accounts)) => {
                                        let acct_strings: Vec<String> =
                                            accounts.into_iter().map(|p| p.to_string()).collect();
                                        quote_attempts.push(format!(
                                            "pump_amm=ok amount_out={} pool={} accounts_len={}",
                                            q.amount_out,
                                            pool_id,
                                            acct_strings.len()
                                        ));
                                        record_candidate(
                                            "pump_amm",
                                            q.amount_out,
                                            pool_id,
                                            acct_strings,
                                        );
                                    }
                                    Ok(None) => {
                                        warn!(mint = %mint, "pump_amm quote returned route, but pool accounts not found; skipping pump_amm");
                                        quote_attempts.push(format!(
                                            "pump_amm=skip no_pool_accounts amount_out={} pool={}",
                                            q.amount_out, pool_id
                                        ));
                                    }
                                    Err(e) => {
                                        warn!(mint = %mint, error = %e, "pump_amm pool account discovery failed; skipping pump_amm");
                                        quote_attempts.push(format!("pump_amm=err_discovery {e}"));
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            quote_attempts.push("pump_amm=none".to_string());
                        }
                        Err(e) => {
                            quote_attempts.push(format!("pump_amm=err {e:#}"));
                        }
                    },
                }

                // Meteora DLMM (requires valid Geyser active_id and pool accounts).
                match meteora
                    .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), amount_in)
                    .await
                {
                    Ok(Some(q)) => {
                        #[cfg(unix)]
                        maybe_ping_watchdog();

                        if let Some(pool_id) = q.route.first().cloned() {
                            if let Ok(pool_pk) = Pubkey::from_str(&pool_id) {
                                if let Some(pool_accounts) = meteora.get_pool_accounts(&pool_pk) {
                                    let active_id = pool_accounts
                                        .get(5)
                                        .and_then(|s| s.strip_prefix("active_id:"))
                                        .and_then(|v| v.parse::<i32>().ok())
                                        .unwrap_or(0);

                                    if active_id != 0 {
                                        quote_attempts.push(format!(
                                            "meteora=ok amount_out={} pool={} active_id={} accounts_len={}",
                                            q.amount_out,
                                            pool_id,
                                            active_id,
                                            pool_accounts.len()
                                        ));
                                        record_candidate(
                                            "meteora_dlmm",
                                            q.amount_out,
                                            pool_id,
                                            pool_accounts,
                                        );
                                    } else {
                                        quote_attempts.push(format!(
                                            "meteora=skip active_id=0 (no Geyser data) pool={}",
                                            pool_id
                                        ));
                                    }
                                } else {
                                    quote_attempts.push(format!(
                                        "meteora=skip no_pool_accounts amount_out={} pool={}",
                                        q.amount_out, pool_id
                                    ));
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        quote_attempts.push("meteora=none".to_string());
                    }
                    Err(e) => {
                        quote_attempts.push(format!("meteora=err {e:#}"));
                    }
                }

                // Raydium (no additional pool accounts needed here).
                match raydium
                    .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), amount_in)
                    .await
                {
                    Ok(Some(q)) => {
                        #[cfg(unix)]
                        maybe_ping_watchdog();

                        if let Some(pool_id) = q.route.first().cloned() {
                            quote_attempts.push(format!(
                                "raydium=ok amount_out={} pool={}",
                                q.amount_out, pool_id
                            ));
                            record_candidate(
                                "raydium",
                                q.amount_out,
                                pool_id,
                                resources.accounts.clone(),
                            );
                        }
                    }
                    Ok(None) => {
                        quote_attempts.push("raydium=none".to_string());
                    }
                    Err(e) => {
                        quote_attempts.push(format!("raydium=err {e:#}"));
                    }
                }

                // Orca Whirlpool (requires pool accounts).
                match orca
                    .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), amount_in)
                    .await
                {
                    Ok(Some(q)) => {
                        #[cfg(unix)]
                        maybe_ping_watchdog();

                        if let Some(pool_id) = q.route.first().cloned() {
                            if let Ok(pool_pk) = Pubkey::from_str(&pool_id) {
                                if let Some(pool_accounts) = orca.get_pool_accounts(&pool_pk) {
                                    quote_attempts.push(format!(
                                        "orca=ok amount_out={} pool={} accounts_len={}",
                                        q.amount_out,
                                        pool_id,
                                        pool_accounts.len()
                                    ));
                                    record_candidate(
                                        "orca",
                                        q.amount_out,
                                        pool_id,
                                        pool_accounts,
                                    );
                                } else {
                                    quote_attempts.push(format!(
                                        "orca=skip no_pool_accounts amount_out={} pool={}",
                                        q.amount_out, pool_id
                                    ));
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        quote_attempts.push("orca=none".to_string());
                    }
                    Err(e) => {
                        quote_attempts.push(format!("orca=err {e:#}"));
                    }
                }

                if let Some(best_route) = select_best_route(candidates) {
                    metadata.insert(
                        "sell_routing".to_string(),
                        "multi_pool".to_string(),
                    );
                    metadata.insert("dex".to_string(), best_route.dex.clone());
                    if let Some(creator) = best_route.creator.clone() {
                        metadata.insert("creator".to_string(), creator);
                    }
                    resources.pools = vec![best_route.pool_id.clone()];
                    resources.accounts = best_route.accounts;
                    min_out_sol = Some(Self::apply_slippage_min_out(
                        best_route.amount_out,
                        max_slippage_bps,
                    ));
                    quote_attempts.push(format!(
                        "multi_pool=best dex={} amount_out={} pool={}",
                        best_route.dex, best_route.amount_out, best_route.pool_id
                    ));
                }
            }

            // Final fallback: LivePoolCache-derived quote + accounts (GEYSER-first).
            if min_out_sol.is_none() {
                if let Some(ref cache) = ctx.live_pool_cache {
                    let sol_mint_pk = sol_mint;
                    let mut candidates: Vec<RouteCandidate> = Vec::new();

                    for (pool_addr, state) in cache.iter() {
                        let has_pair = match &state {
                            CachedPoolState::PumpFun(s) => {
                                s.token_mint == mint && s.creator != Pubkey::default()
                            }
                            CachedPoolState::PumpAmm(s) => {
                                (s.base_mint == mint && s.quote_mint == sol_mint_pk)
                                    || (s.quote_mint == mint && s.base_mint == sol_mint_pk)
                            }
                            CachedPoolState::RaydiumAmm(s) => {
                                (s.base_mint == mint && s.quote_mint == sol_mint_pk)
                                    || (s.quote_mint == mint && s.base_mint == sol_mint_pk)
                            }
                            CachedPoolState::RaydiumCpmm(s) => {
                                (s.token_0_mint == mint && s.token_1_mint == sol_mint_pk)
                                    || (s.token_1_mint == mint && s.token_0_mint == sol_mint_pk)
                            }
                            CachedPoolState::Meteora(s) => {
                                (s.token_x_mint == mint && s.token_y_mint == sol_mint_pk)
                                    || (s.token_y_mint == mint && s.token_x_mint == sol_mint_pk)
                            }
                            CachedPoolState::MeteoraCpmm(s) => {
                                (s.token_0_mint == mint && s.token_1_mint == sol_mint_pk)
                                    || (s.token_1_mint == mint && s.token_0_mint == sol_mint_pk)
                            }
                            CachedPoolState::Orca(s) => {
                                (s.token_mint_a == mint && s.token_mint_b == sol_mint_pk)
                                    || (s.token_mint_b == mint && s.token_mint_a == sol_mint_pk)
                            }
                        };

                        if !has_pair {
                            continue;
                        }

                        let mut intent_for_quote = TradeIntent::new_sell(
                            "execution-engine",
                            BUILD_VERSION,
                            &ctx.run_id,
                            format!("liquidation-cache-{}", Uuid::new_v4()),
                            "execution-engine",
                            IntentTier::Tier0,
                            IntentOrigin::ExecutionMevB,
                            mint.to_string(),
                            decimals,
                            sol_mint.to_string(),
                            amount_in,
                            0,
                            max_slippage_bps,
                            TradingRegime::NotApplicable,
                        );
                        intent_for_quote.resources.pools = vec![pool_addr.to_string()];

                        let min_out = match quote_calculator::calculate_fresh_min_out(
                            cache,
                            &intent_for_quote,
                        ) {
                            Ok(Some(v)) => v,
                            Ok(None) => {
                                quote_attempts.push(format!(
                                    "cache=skip no_quote pool={}",
                                    pool_addr
                                ));
                                continue;
                            }
                            Err(e) => {
                                quote_attempts.push(format!(
                                    "cache=err pool={} err={}",
                                    pool_addr, e
                                ));
                                continue;
                            }
                        };

                        let dex = state.dex_name().to_string();
                        let pool_id = pool_addr.to_string();

                        let accounts = match &state {
                            CachedPoolState::PumpAmm(s) => {
                                if s.pool_accounts.is_empty() {
                                    quote_attempts.push(format!(
                                        "cache=skip pump_amm no_pool_accounts pool={}",
                                        pool_id
                                    ));
                                    continue;
                                }
                                if s.pool_accounts[0].to_string() != pool_id {
                                    quote_attempts.push(format!(
                                        "cache=skip pump_amm pool_mismatch pool={}",
                                        pool_id
                                    ));
                                    continue;
                                }
                                s.pool_accounts.iter().map(|p| p.to_string()).collect()
                            }
                            CachedPoolState::Meteora(_) => {
                                let pool_pk = match Pubkey::from_str(&pool_id) {
                                    Ok(pk) => pk,
                                    Err(_) => continue,
                                };
                                let Some(pool_accounts) = meteora.get_pool_accounts(&pool_pk) else {
                                    quote_attempts.push(format!(
                                        "cache=skip meteora no_pool_accounts pool={}",
                                        pool_id
                                    ));
                                    continue;
                                };
                                let active_id = pool_accounts
                                    .get(5)
                                    .and_then(|s| s.strip_prefix("active_id:"))
                                    .and_then(|v| v.parse::<i32>().ok())
                                    .unwrap_or(0);
                                if active_id == 0 {
                                    quote_attempts.push(format!(
                                        "cache=skip meteora active_id=0 pool={}",
                                        pool_id
                                    ));
                                    continue;
                                }
                                pool_accounts
                            }
                            CachedPoolState::Orca(_) => {
                                let pool_pk = match Pubkey::from_str(&pool_id) {
                                    Ok(pk) => pk,
                                    Err(_) => continue,
                                };
                                let Some(pool_accounts) = orca.get_pool_accounts(&pool_pk) else {
                                    quote_attempts.push(format!(
                                        "cache=skip orca no_pool_accounts pool={}",
                                        pool_id
                                    ));
                                    continue;
                                };
                                pool_accounts
                            }
                            _ => resources.accounts.clone(),
                        };

                        candidates.push(RouteCandidate {
                            dex,
                            amount_out: min_out,
                            pool_id,
                            accounts,
                            creator: match &state {
                                CachedPoolState::PumpFun(s) => Some(s.creator.to_string()),
                                _ => None,
                            },
                        });
                    }

                    if let Some(best_route) = select_best_route(candidates) {
                        metadata.insert(
                            "sell_routing".to_string(),
                            "multi_pool".to_string(),
                        );
                        metadata.insert("dex".to_string(), best_route.dex.clone());
                        if let Some(creator) = best_route.creator.clone() {
                            metadata.insert("creator".to_string(), creator);
                        }
                        resources.pools = vec![best_route.pool_id.clone()];
                        resources.accounts = best_route.accounts;
                        min_out_sol = Some(best_route.amount_out);
                        quote_attempts.push(format!(
                            "cache=best dex={} min_out={} pool={}",
                            best_route.dex, best_route.amount_out, best_route.pool_id
                        ));
                    }
                }
            }

            let Some(min_out) = min_out_sol else {
                warn!(mint = %mint, amount_in, token_account = %ta_pubkey, "No supported route for liquidation; skipping");

                // Emit a rejected DecisionRecord so the reason is forensically visible even
                // when we cannot generate a sell intent (no quote / no supported route).
                // This is especially important for “why was token X not liquidated?” cases.
                let skip_intent_id = format!("liquidation-skip-{}", Uuid::new_v4());
                let mut skip_intent = TradeIntent::new_sell(
                    "execution-engine",
                    BUILD_VERSION,
                    &ctx.run_id,
                    skip_intent_id,
                    "execution-engine",
                    IntentTier::Tier0,
                    IntentOrigin::ExecutionMevB,
                    mint.to_string(),
                    decimals,
                    sol_mint.to_string(),
                    amount_in,
                    0,
                    max_slippage_bps,
                    TradingRegime::NotApplicable,
                );
                skip_intent.ttl_ms = Some(ttl_ms);
                skip_intent.resources = resources;
                skip_intent.metadata.extend(metadata);

                let decision_id = ctx.next_decision_id();
                let checks = vec![CheckResult {
                    check_name: "liquidation_quote".to_string(),
                    passed: false,
                    reason_code: Some(RejectReason::QuoteUnavailable.to_string()),
                    details: Some(format!(
                        "no_quote_from_supported_dexes mint={} amount_in={} token_account={} attempts=[{}]",
                        mint,
                        amount_in,
                        ta_pubkey,
                        quote_attempts.join(" | ")
                    )),
                }];

                let _ = emit_rejected_decision(
                    ctx.as_ref(),
                    decision_id,
                    &skip_intent,
                    checks,
                    RejectReason::QuoteUnavailable,
                )
                .await;
                continue;
            };

            let intent_id = format!("liquidation-{}", Uuid::new_v4());
            let mut intent = TradeIntent::new_sell(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
                intent_id,
                "execution-engine",
                IntentTier::Tier0,
                IntentOrigin::ExecutionMevB,
                mint.to_string(),
                decimals,
                sol_mint.to_string(),
                amount_in,
                0,
                max_slippage_bps,
                TradingRegime::NotApplicable,
            );
            intent.ttl_ms = Some(ttl_ms);
            intent.resources = resources;
            intent.execution = Some(TradeExecutionConstraints {
                min_out: Some(ExplicitAmount::new(min_out, 9)),
            });
            intent.metadata.extend(metadata);

            liquidation_intents.push(intent);
        }

        info!(
            count = liquidation_intents.len(),
            "Liquidation intents prepared"
        );
        for intent in liquidation_intents {
            #[cfg(unix)]
            maybe_ping_watchdog();

            if let Err(e) = process_intent(&ctx, intent).await {
                warn!(error = %e, "Liquidation intent processing failed");
            }
        }

        // Post-liquidation cleanup:
        // - Unwrap WSOL by closing WSOL ATA
        // - Close empty token accounts to avoid leaving rent-funded accounts open
        // Best-effort: failures are logged but do not fail the liquidation job.
        //
        // IMPORTANT: Wait for token sale TXs to confirm before cleanup!
        // send_transaction_rpc is fire-and-forget (no confirmation wait).
        // Without this delay, cleanup may run before token sales are confirmed,
        // causing WSOL ATA to be closed based on stale state, then token sales
        // create new WSOL that remains in the wallet.
        info!("Waiting for liquidation TXs to confirm before cleanup...");
        tokio::time::sleep(Duration::from_secs(15)).await;
        #[cfg(unix)]
        maybe_ping_watchdog();
        if let Err(e) = Self::cleanup_wallet_after_liquidation(ctx.as_ref(), owner).await {
            warn!(error = %e, "Liquidation cleanup failed (best-effort)");
        }

        info!("Liquidation job completed");
    }

    async fn cleanup_wallet_after_liquidation(
        ctx: &ExecutionContext,
        wallet: Pubkey,
    ) -> Result<()> {
        let token_program_id = Pubkey::new_from_array(spl_token::id().to_bytes());
        let token_2022_program_id = Pubkey::new_from_array(spl_token_2022::id().to_bytes());
        let wsol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
            .expect("valid WSOL mint");

        // Refresh list of token accounts so we operate on up-to-date balances.
        let mut token_accounts = ctx
            .rpc
            .rpc
            .get_token_accounts_by_owner(&wallet, TokenAccountsFilter::ProgramId(token_program_id))
            .await
            .unwrap_or_default();

        if let Ok(mut accounts_2022) = ctx
            .rpc
            .rpc
            .get_token_accounts_by_owner(
                &wallet,
                TokenAccountsFilter::ProgramId(token_2022_program_id),
            )
            .await
        {
            token_accounts.append(&mut accounts_2022);
        }

        // 1) Unwrap WSOL: close WSOL ATA (classic spl-token only)
        let wsol_ata = ExecutionContext::ata_for_owner_mint(&wallet, &wsol_mint, &token_program_id);
        if let Ok(acc) = ctx.rpc.rpc.get_account(&wsol_ata).await {
            if acc.owner == token_program_id {
                let close_ix = ExecutionContext::prog_ix_to_sdk(spl_ix::close_account(
                    &spl_token::id(),
                    &ExecutionContext::sdk_to_spl(&wsol_ata),
                    &ExecutionContext::sdk_to_spl(&wallet),
                    &ExecutionContext::sdk_to_spl(&wallet),
                    &[],
                )?);

                let plan = tx_builder::TxPlan {
                    instructions: vec![close_ix],
                };
                let sim = simulate_transaction(ctx, wallet, &plan).await;
                if sim.success {
                    let config = ctx.get_config();
                    if config.send_enabled {
                        match send_transaction_rpc(
                            ctx,
                            wallet,
                            &plan,
                            config.send_skip_preflight,
                            parse_commitment_level_opt(config.send_preflight_commitment.as_deref()),
                        )
                        .await
                        {
                            Ok(sig) => {
                                info!(wallet = %wallet, wsol_ata = %wsol_ata, signature = %sig, "Unwrapped WSOL (closed ATA)")
                            }
                            Err(e) => {
                                warn!(wallet = %wallet, wsol_ata = %wsol_ata, error = %e, "Failed to unwrap WSOL (close ATA send failed)")
                            }
                        }
                    } else {
                        info!(wallet = %wallet, wsol_ata = %wsol_ata, "send_enabled=false; would unwrap WSOL by closing ATA");
                    }
                } else {
                    warn!(wallet = %wallet, wsol_ata = %wsol_ata, error = ?sim.error_code, "WSOL unwrap simulation failed; not sending");
                }
            }
        }

        // 2) Close empty token accounts (best-effort)
        let mut close_candidates: Vec<(Pubkey, Pubkey, Pubkey, u64)> = Vec::new();
        for ta in token_accounts {
            let ta_pubkey = match Pubkey::from_str(&ta.pubkey) {
                Ok(p) => p,
                Err(_) => continue,
            };

            // Skip WSOL ATA here (handled above)
            if ta_pubkey == wsol_ata {
                continue;
            }

            // Extract mint+amount from JsonParsed only.
            let parsed = match ta.account.data {
                UiAccountData::Json(parsed) => parsed,
                _ => continue,
            };
            let serde_json::Value::Object(root) = parsed.parsed else {
                continue;
            };
            let info = match root.get("info") {
                Some(v) => v,
                None => continue,
            };
            let mint_str = match info.get("mint").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let amount_str = info
                .get("tokenAmount")
                .and_then(|v| v.get("amount"))
                .and_then(|v| v.as_str())
                .unwrap_or("0");

            let mint = match Pubkey::from_str(mint_str) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let amount_raw: u64 = match amount_str.parse() {
                Ok(a) => a,
                Err(_) => continue,
            };

            // Only close empty accounts.
            if amount_raw != 0 {
                continue;
            }
            if mint == wsol_mint {
                continue;
            }

            let token_program = Pubkey::from_str(&ta.account.owner).unwrap_or(token_program_id);
            if token_program != token_program_id && token_program != token_2022_program_id {
                continue;
            }

            close_candidates.push((ta_pubkey, mint, token_program, amount_raw));
        }

        if close_candidates.is_empty() {
            return Ok(());
        }

        info!(
            count = close_candidates.len(),
            "Closing empty token accounts (best-effort)"
        );
        for (token_account, mint, token_program, _amount_raw) in close_candidates {
            let close_ix = if token_program == token_program_id {
                ExecutionContext::prog_ix_to_sdk(spl_ix::close_account(
                    &spl_token::id(),
                    &ExecutionContext::sdk_to_spl(&token_account),
                    &ExecutionContext::sdk_to_spl(&wallet),
                    &ExecutionContext::sdk_to_spl(&wallet),
                    &[],
                )?)
            } else {
                ExecutionContext::prog_ix_to_sdk(spl22_ix::close_account(
                    &spl_token_2022::id(),
                    &ExecutionContext::sdk_to_spl(&token_account),
                    &ExecutionContext::sdk_to_spl(&wallet),
                    &ExecutionContext::sdk_to_spl(&wallet),
                    &[],
                )?)
            };

            let plan = tx_builder::TxPlan {
                instructions: vec![close_ix],
            };

            let sim = simulate_transaction(ctx, wallet, &plan).await;
            if !sim.success {
                warn!(token_account = %token_account, mint = %mint, token_program = %token_program, error = ?sim.error_code, "Close empty token account simulation failed; not sending");
                continue;
            }

            let config = ctx.get_config();
            if !config.send_enabled {
                info!(token_account = %token_account, mint = %mint, token_program = %token_program, "send_enabled=false; would close empty token account");
                continue;
            }

            match send_transaction_rpc(
                ctx,
                wallet,
                &plan,
                config.send_skip_preflight,
                parse_commitment_level_opt(config.send_preflight_commitment.as_deref()),
            )
            .await
            {
                Ok(sig) => {
                    info!(token_account = %token_account, mint = %mint, token_program = %token_program, signature = %sig, "Closed empty token account")
                }
                Err(e) => {
                    warn!(token_account = %token_account, mint = %mint, token_program = %token_program, error = %e, "Close empty token account send failed")
                }
            }
        }

        Ok(())
    }

    async fn run_manual_burn_job(
        ctx: Arc<ExecutionContext>,
        request_id: String,
        owner_pubkey: String,
        token_accounts: Vec<String>,
        close_accounts: bool,
        reason: Option<String>,
    ) {
        #[cfg(unix)]
        let mut last_watchdog_ping = std::time::Instant::now();
        #[cfg(unix)]
        let mut maybe_ping_watchdog = || {
            if last_watchdog_ping.elapsed() >= Duration::from_secs(5) {
                let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
                last_watchdog_ping = std::time::Instant::now();
            }
        };

        if ctx
            .burn_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            warn!(request_id = %request_id, "Manual burn already in progress; ignoring new request");
            return;
        }

        struct BurnInProgressGuard {
            ctx: Arc<ExecutionContext>,
        }
        impl Drop for BurnInProgressGuard {
            fn drop(&mut self) {
                self.ctx.burn_in_progress.store(false, Ordering::SeqCst);
            }
        }
        let _guard = BurnInProgressGuard {
            ctx: Arc::clone(&ctx),
        };

        let Some(wallet) = ctx.wallet_pubkey else {
            warn!(request_id = %request_id, "Manual burn requested but wallet_pubkey is None");
            return;
        };
        if ctx.treasury.is_none() {
            warn!(request_id = %request_id, "Manual burn requested but treasury (signer) is None");
            return;
        }

        if owner_pubkey != wallet.to_string() {
            warn!(request_id = %request_id, expected_wallet = %wallet, provided_wallet = %owner_pubkey, "Manual burn owner_pubkey mismatch; refusing");
            return;
        }

        info!(request_id = %request_id, wallet = %wallet, count = token_accounts.len(), close_accounts, reason = ?reason, "Starting manual burn job");
        #[cfg(unix)]
        maybe_ping_watchdog();

        // Initialize DEX connectors for route validation.
        let raydium = Raydium::new(Arc::clone(&ctx.rpc));
        let pumpfun = match PumpFunDex::new(Arc::clone(&ctx.rpc)) {
            Ok(p) => Some(p),
            Err(e) => {
                warn!(error = %e, "Failed to init PumpFunDex in burn job; continuing with Raydium only");
                None
            }
        };
        if let Err(e) = raydium.refresh_pools().await {
            warn!(error = %e, "Raydium refresh_pools failed in burn job; route validation may miss routes");
        }
        #[cfg(unix)]
        maybe_ping_watchdog();

        let spl = Pubkey::new_from_array(spl_token::id().to_bytes());
        let spl22 = Pubkey::new_from_array(spl_token_2022::id().to_bytes());
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
            .expect("valid SOL mint");

        for ta_str in token_accounts {
            #[cfg(unix)]
            maybe_ping_watchdog();

            let token_account_pk = match Pubkey::from_str(&ta_str) {
                Ok(p) => p,
                Err(e) => {
                    warn!(request_id = %request_id, token_account = %ta_str, error = %e, "Invalid token account pubkey; skipping");
                    continue;
                }
            };

            let acct = match ctx.rpc.rpc.get_account(&token_account_pk).await {
                Ok(a) => a,
                Err(e) => {
                    warn!(request_id = %request_id, token_account = %token_account_pk, error = %e, "Failed to fetch token account; skipping");
                    continue;
                }
            };

            let token_program = acct.owner;
            if token_program != spl && token_program != spl22 {
                warn!(request_id = %request_id, token_account = %token_account_pk, token_program = %token_program, "Token account is not owned by SPL Token or Token-2022; skipping");
                continue;
            }

            let (mint, owner, amount_raw) = if token_program == spl {
                match spl_token::state::Account::unpack(&acct.data) {
                    Ok(a) => (
                        Self::spl_to_sdk(&a.mint),
                        Self::spl_to_sdk(&a.owner),
                        a.amount,
                    ),
                    Err(e) => {
                        warn!(request_id = %request_id, token_account = %token_account_pk, error = %e, "Failed to unpack SPL token account; skipping");
                        continue;
                    }
                }
            } else {
                match Spl22StateWithExtensions::<Spl22TokenAccount>::unpack(&acct.data) {
                    Ok(a) => {
                        let base = a.base;
                        (
                            Self::spl_to_sdk(&base.mint),
                            Self::spl_to_sdk(&base.owner),
                            base.amount,
                        )
                    }
                    Err(e) => {
                        warn!(request_id = %request_id, token_account = %token_account_pk, error = %e, "Failed to unpack token-2022 account; skipping");
                        continue;
                    }
                }
            };

            if owner != wallet {
                warn!(request_id = %request_id, token_account = %token_account_pk, owner = %owner, expected_owner = %wallet, "Token account owner mismatch; skipping");
                continue;
            }
            if mint == sol_mint {
                warn!(request_id = %request_id, token_account = %token_account_pk, "Refusing to burn SOL/WSOL mint");
                continue;
            }

            // Re-validate: if a supported sell route exists, refuse to burn.
            let decimals = get_token_decimals_or_default(ctx.rpc.as_ref(), &mint).await;
            let unit_u64 = 10u128
                .checked_pow(decimals as u32)
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(1);
            let quote_amount =
                std::cmp::min(std::cmp::max(1, unit_u64), std::cmp::max(1, amount_raw));

            let mut route_exists = false;

            if let Some(ref pumpfun) = pumpfun {
                if let Ok(Some(q)) = pumpfun
                    .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), quote_amount)
                    .await
                {
                    #[cfg(unix)]
                    maybe_ping_watchdog();

                    // Only treat as a real Pump.fun route if we can parse creator from the bonding curve.
                    if let Some(bc) = q.route.first().and_then(|s| Pubkey::from_str(s).ok()) {
                        if let Ok(acct) = ctx.rpc.rpc.get_account(&bc).await {
                            if BondingCurveState::parse(&acct.data).is_ok() {
                                route_exists = true;
                            }
                        }
                    }
                }
            }

            if !route_exists {
                if let Ok(Some(_q)) = raydium
                    .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), quote_amount)
                    .await
                {
                    route_exists = true;
                }
            }

            if route_exists {
                warn!(request_id = %request_id, token_account = %token_account_pk, mint = %mint, amount_raw, "Sell route exists; refusing to burn");
                let rec = BurnOpRecord {
                    header: RecordHeader::new("execution-engine", BUILD_VERSION, &ctx.run_id),
                    request_id: request_id.clone(),
                    wallet: wallet.to_string(),
                    token_account: token_account_pk.to_string(),
                    mint: mint.to_string(),
                    token_program: token_program.to_string(),
                    amount_raw,
                    close_accounts,
                    outcome: "refused_route_exists".to_string(),
                    signature: None,
                    error: None,
                    reason: reason.clone(),
                };
                let _ = ctx.burn_writer.write(&rec);
                continue;
            }

            // Build burn (if amount>0) + close instructions.
            let mut ixs: Vec<solana_sdk::instruction::Instruction> = Vec::new();

            if amount_raw > 0 {
                let burn_ix_prog = if token_program == spl {
                    spl_ix::burn(
                        &spl_token::id(),
                        &Self::sdk_to_spl(&token_account_pk),
                        &Self::sdk_to_spl(&mint),
                        &Self::sdk_to_spl(&wallet),
                        &[],
                        amount_raw,
                    )
                } else {
                    spl22_ix::burn(
                        &spl_token_2022::id(),
                        &Self::sdk_to_spl(&token_account_pk),
                        &Self::sdk_to_spl(&mint),
                        &Self::sdk_to_spl(&wallet),
                        &[],
                        amount_raw,
                    )
                };

                match burn_ix_prog {
                    Ok(ix) => ixs.push(Self::prog_ix_to_sdk(ix)),
                    Err(e) => {
                        warn!(request_id = %request_id, token_account = %token_account_pk, mint = %mint, error = %e, "Failed to build burn instruction; skipping");
                        let rec = BurnOpRecord {
                            header: RecordHeader::new(
                                "execution-engine",
                                BUILD_VERSION,
                                &ctx.run_id,
                            ),
                            request_id: request_id.clone(),
                            wallet: wallet.to_string(),
                            token_account: token_account_pk.to_string(),
                            mint: mint.to_string(),
                            token_program: token_program.to_string(),
                            amount_raw,
                            close_accounts,
                            outcome: "failed_build_burn_ix".to_string(),
                            signature: None,
                            error: Some(format!("{e}")),
                            reason: reason.clone(),
                        };
                        let _ = ctx.burn_writer.write(&rec);
                        continue;
                    }
                }
            }

            if close_accounts {
                let close_ix_prog = if token_program == spl {
                    spl_ix::close_account(
                        &spl_token::id(),
                        &Self::sdk_to_spl(&token_account_pk),
                        &Self::sdk_to_spl(&wallet),
                        &Self::sdk_to_spl(&wallet),
                        &[],
                    )
                } else {
                    spl22_ix::close_account(
                        &spl_token_2022::id(),
                        &Self::sdk_to_spl(&token_account_pk),
                        &Self::sdk_to_spl(&wallet),
                        &Self::sdk_to_spl(&wallet),
                        &[],
                    )
                };

                match close_ix_prog {
                    Ok(ix) => ixs.push(Self::prog_ix_to_sdk(ix)),
                    Err(e) => {
                        warn!(request_id = %request_id, token_account = %token_account_pk, mint = %mint, error = %e, "Failed to build close instruction; skipping");
                        let rec = BurnOpRecord {
                            header: RecordHeader::new(
                                "execution-engine",
                                BUILD_VERSION,
                                &ctx.run_id,
                            ),
                            request_id: request_id.clone(),
                            wallet: wallet.to_string(),
                            token_account: token_account_pk.to_string(),
                            mint: mint.to_string(),
                            token_program: token_program.to_string(),
                            amount_raw,
                            close_accounts,
                            outcome: "failed_build_close_ix".to_string(),
                            signature: None,
                            error: Some(format!("{e}")),
                            reason: reason.clone(),
                        };
                        let _ = ctx.burn_writer.write(&rec);
                        continue;
                    }
                }
            }

            if ixs.is_empty() {
                let rec = BurnOpRecord {
                    header: RecordHeader::new("execution-engine", BUILD_VERSION, &ctx.run_id),
                    request_id: request_id.clone(),
                    wallet: wallet.to_string(),
                    token_account: token_account_pk.to_string(),
                    mint: mint.to_string(),
                    token_program: token_program.to_string(),
                    amount_raw,
                    close_accounts,
                    outcome: "no_op".to_string(),
                    signature: None,
                    error: None,
                    reason: reason.clone(),
                };
                let _ = ctx.burn_writer.write(&rec);
                continue;
            }

            let plan = tx_builder::TxPlan { instructions: ixs };
            let sim = simulate_transaction(&ctx, wallet, &plan).await;
            if !sim.success {
                warn!(request_id = %request_id, token_account = %token_account_pk, mint = %mint, error = ?sim.error_code, "Burn simulation failed; not sending");
                let rec = BurnOpRecord {
                    header: RecordHeader::new("execution-engine", BUILD_VERSION, &ctx.run_id),
                    request_id: request_id.clone(),
                    wallet: wallet.to_string(),
                    token_account: token_account_pk.to_string(),
                    mint: mint.to_string(),
                    token_program: token_program.to_string(),
                    amount_raw,
                    close_accounts,
                    outcome: "sim_failed".to_string(),
                    signature: None,
                    error: sim.error_code,
                    reason: reason.clone(),
                };
                let _ = ctx.burn_writer.write(&rec);
                continue;
            }

            let config = ctx.get_config();
            if !config.send_enabled {
                info!(request_id = %request_id, token_account = %token_account_pk, mint = %mint, "send_enabled=false; burn simulated ok but not sending");
                let rec = BurnOpRecord {
                    header: RecordHeader::new("execution-engine", BUILD_VERSION, &ctx.run_id),
                    request_id: request_id.clone(),
                    wallet: wallet.to_string(),
                    token_account: token_account_pk.to_string(),
                    mint: mint.to_string(),
                    token_program: token_program.to_string(),
                    amount_raw,
                    close_accounts,
                    outcome: "send_disabled".to_string(),
                    signature: None,
                    error: None,
                    reason: reason.clone(),
                };
                let _ = ctx.burn_writer.write(&rec);
                continue;
            }

            #[cfg(unix)]
            maybe_ping_watchdog();

            match send_transaction_rpc(
                &ctx,
                wallet,
                &plan,
                config.send_skip_preflight,
                parse_commitment_level_opt(config.send_preflight_commitment.as_deref()),
            )
            .await
            {
                Ok(sig) => {
                    info!(request_id = %request_id, token_account = %token_account_pk, mint = %mint, signature = %sig, "Burn transaction sent");
                    let rec = BurnOpRecord {
                        header: RecordHeader::new("execution-engine", BUILD_VERSION, &ctx.run_id),
                        request_id: request_id.clone(),
                        wallet: wallet.to_string(),
                        token_account: token_account_pk.to_string(),
                        mint: mint.to_string(),
                        token_program: token_program.to_string(),
                        amount_raw,
                        close_accounts,
                        outcome: "sent".to_string(),
                        signature: Some(sig),
                        error: None,
                        reason: reason.clone(),
                    };
                    let _ = ctx.burn_writer.write(&rec);
                }
                Err(e) => {
                    warn!(request_id = %request_id, token_account = %token_account_pk, mint = %mint, error = %e, "Burn send failed");
                    let rec = BurnOpRecord {
                        header: RecordHeader::new("execution-engine", BUILD_VERSION, &ctx.run_id),
                        request_id: request_id.clone(),
                        wallet: wallet.to_string(),
                        token_account: token_account_pk.to_string(),
                        mint: mint.to_string(),
                        token_program: token_program.to_string(),
                        amount_raw,
                        close_accounts,
                        outcome: "send_failed".to_string(),
                        signature: None,
                        error: Some(e),
                        reason: reason.clone(),
                    };
                    let _ = ctx.burn_writer.write(&rec);
                }
            }
        }

        info!(request_id = %request_id, "Manual burn job finished");
    }
    /// Update config and return response (P1: Runtime Configuration via UI)
    fn apply_config_update(&self, update: &ConfigUpdate) -> ConfigUpdateResponse {
        let mut config = self.config.write();
        let mut applied = Vec::new();
        let mut rejected = Vec::new();

        // Process each config key
        for (key, value) in &update.config {
            match key.as_str() {
                "max_position_size_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.max_position_size_lamports = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "daily_loss_limit_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.daily_loss_limit_lamports = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "max_open_positions" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 100 {
                            config.max_open_positions = v as usize;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-100".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "max_slippage_bps" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 10000 {
                            config.max_slippage_bps = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-10000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "simulation_timeout_ms" => {
                    if let Some(v) = value.as_u64() {
                        if (100..=30000).contains(&v) {
                            config.simulation_timeout_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 100-30000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "confirmation_timeout_ms" => {
                    if let Some(v) = value.as_u64() {
                        if (500..=300_000).contains(&v) {
                            config.confirmation_timeout_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 500-300000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "confirm_timeout_ms" => {
                    if let Some(v) = value.as_u64() {
                        if (500..=300_000).contains(&v) {
                            config.confirmation_timeout_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 500-300000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "skip_preflight" => {
                    if let Some(v) = value.as_bool() {
                        config.send_skip_preflight = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "preflight_commitment" => {
                    if value.is_null() {
                        config.send_preflight_commitment = None;
                        applied.push(key.clone());
                        info!(key = %key, new_value = "null", "Config updated");
                    } else if let Some(v) = value.as_str() {
                        let v_lc = v.to_lowercase();
                        match v_lc.as_str() {
                            "processed" | "confirmed" | "finalized" => {
                                config.send_preflight_commitment = Some(v_lc);
                                applied.push(key.clone());
                                info!(key = %key, new_value = %v, "Config updated");
                            }
                            _ => rejected.push((
                                key.clone(),
                                "Must be one of: processed, confirmed, finalized (or null)"
                                    .to_string(),
                            )),
                        }
                    } else {
                        rejected.push((
                            key.clone(),
                            "Invalid type, expected string or null".to_string(),
                        ));
                    }
                }
                "send_enabled" => {
                    if let Some(v) = value.as_bool() {
                        // Only allow enabling if keys are configured
                        let has_keys = Treasury::load_from_env().is_ok();

                        if v && !has_keys {
                            rejected.push((
                                key.clone(),
                                "Cannot enable sending without wallet keys".to_string(),
                            ));
                        } else {
                            config.send_enabled = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                // === WSOL Manager Config ===
                "wsol_enabled" => {
                    if let Some(v) = value.as_bool() {
                        config.wsol_enabled = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "wsol_min_wsol_sol" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=100.0).contains(&v) {
                            config.wsol_min_wsol_sol = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 0-100".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "wsol_target_wsol_sol" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=100.0).contains(&v) {
                            config.wsol_target_wsol_sol = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 0-100".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "wsol_max_wsol_sol" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=100.0).contains(&v) {
                            config.wsol_max_wsol_sol = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 0-100".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "wsol_min_native_sol" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=10.0).contains(&v) {
                            config.wsol_min_native_sol = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 0-10".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "wsol_cooldown_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v <= 3600 {
                            config.wsol_cooldown_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 0-3600".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "wsol_dry_run" => {
                    if let Some(v) = value.as_bool() {
                        config.wsol_dry_run = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                // === Account Janitor Config ===
                "janitor_enabled" => {
                    if let Some(v) = value.as_bool() {
                        config.janitor_enabled = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "janitor_close_ata_interval_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v >= 60 {
                            config.janitor_close_ata_interval_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be >= 60".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "janitor_close_ata_min_age_secs" => {
                    if let Some(v) = value.as_u64() {
                        config.janitor_close_ata_min_age_secs = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "janitor_close_ata_max_per_run" => {
                    if let Some(v) = value.as_u64() {
                        if (1..=100).contains(&v) {
                            config.janitor_close_ata_max_per_run = v as usize;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-100".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "janitor_merge_dust_enabled" => {
                    if let Some(v) = value.as_bool() {
                        config.janitor_merge_dust_enabled = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "janitor_merge_dust_interval_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v >= 60 {
                            config.janitor_merge_dust_interval_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be >= 60".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "janitor_merge_dust_max_per_run" => {
                    if let Some(v) = value.as_u64() {
                        if (1..=100).contains(&v) {
                            config.janitor_merge_dust_max_per_run = v as usize;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-100".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "janitor_swap_dust_enabled" => {
                    if let Some(v) = value.as_bool() {
                        config.janitor_swap_dust_enabled = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "janitor_swap_dust_interval_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v >= 60 {
                            config.janitor_swap_dust_interval_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be >= 60".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "janitor_swap_dust_min_value_sol" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.janitor_swap_dust_min_value_sol = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 0-1".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "janitor_swap_dust_max_slippage_bps" => {
                    if let Some(v) = value.as_u64() {
                        if (1..=10000).contains(&v) {
                            config.janitor_swap_dust_max_slippage_bps = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-10000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "janitor_swap_dust_max_per_run" => {
                    if let Some(v) = value.as_u64() {
                        if (1..=100).contains(&v) {
                            config.janitor_swap_dust_max_per_run = v as usize;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-100".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "janitor_dry_run" => {
                    if let Some(v) = value.as_bool() {
                        config.janitor_dry_run = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                _ => {
                    rejected.push((key.clone(), format!("Unknown config key: {}", key)));
                }
            }
        }

        // Update snapshot ID
        let new_snapshot_id = config.snapshot_id();
        *self.config_snapshot_id.write() = new_snapshot_id.clone();

        // Determine status
        let status = if rejected.is_empty() {
            ConfigUpdateStatus::Applied
        } else if applied.is_empty() {
            ConfigUpdateStatus::Rejected
        } else {
            ConfigUpdateStatus::PartiallyApplied
        };

        ConfigUpdateResponse {
            status,
            applied_keys: applied,
            rejected_keys: rejected,
            new_snapshot_id: Some(new_snapshot_id),
        }
    }

    /// Save state snapshot for crash recovery (P1: DoD K)
    fn save_state(&self) -> Result<()> {
        let snapshot = StateSnapshot::from_context(self);
        snapshot.save(self.log_base.as_path())
    }

    fn next_decision_id(&self) -> String {
        let n = self
            .decision_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("dec-{}-{:06}", &self.run_id[..8], n)
    }

    fn next_execution_id(&self) -> String {
        let n = self
            .execution_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("exe-{}-{:06}", &self.run_id[..8], n)
    }

    fn record_intent_received(&self) {
        self.intents_received
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_intent_rejected(&self) {
        self.intents_rejected
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_sim_failure(&self) {
        self.sim_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // Risk Invariant helpers

    /// Check if we need to reset daily counters (new day)
    fn maybe_reset_daily(&self) {
        let today = chrono::Utc::now().date_naive();
        let mut current = self.current_day.write();
        if *current != today {
            tracing::info!(old_day = %current, new_day = %today, "Daily reset triggered");
            *current = today;
            self.daily_loss_lamports
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Record a loss (positive = loss, negative = profit)
    #[allow(dead_code)]
    fn record_pnl_lamports(&self, pnl: i64) {
        // Positive pnl = loss, negative = profit
        self.daily_loss_lamports
            .fetch_add(pnl, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get current daily loss
    fn get_daily_loss_lamports(&self) -> i64 {
        self.daily_loss_lamports
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Increment open positions
    fn increment_open_positions(&self) {
        self.open_positions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Decrement open positions
    fn decrement_open_positions(&self) {
        // Saturating decrement to avoid underflow.
        let mut current = self
            .open_positions
            .load(std::sync::atomic::Ordering::Relaxed);

        loop {
            if current == 0 {
                return;
            }
            match self.open_positions.compare_exchange_weak(
                current,
                current - 1,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Get current open positions count
    fn get_open_positions(&self) -> usize {
        self.open_positions
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn sdk_to_spl(pk: &Pubkey) -> spl_token::solana_program::pubkey::Pubkey {
    spl_token::solana_program::pubkey::Pubkey::new_from_array(pk.to_bytes())
}

fn spl_to_sdk(pk: &spl_token::solana_program::pubkey::Pubkey) -> Pubkey {
    Pubkey::new_from_array(pk.to_bytes())
}

#[allow(dead_code)]
fn token_program_for_mint_owner(owner: &Pubkey) -> Option<Pubkey> {
    let spl_token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
    let spl_token_2022_program = Pubkey::new_from_array(spl_token_2022::id().to_bytes());

    if *owner == spl_token_program {
        Some(spl_token_program)
    } else if *owner == spl_token_2022_program {
        Some(spl_token_2022_program)
    } else {
        None
    }
}

/// Build minimal CachedPoolState from PoolCacheUpdate (for JetStream bootstrap/sync)
///
/// Since PoolCacheUpdate only contains reserves (not full account data), we create
/// minimal state structures. Full account updates from Geyser will refresh these later.
fn build_minimal_pool_state(update: &PoolCacheUpdate) -> Option<(Pubkey, CachedPoolState)> {
    use ironcrab::execution::live_pool_cache::{
        MeteoraState, OrcaWhirlpoolState, PumpAmmState, PumpFunState, RaydiumAmmState,
        RaydiumCpmmState,
    };

    let pool_addr = match Pubkey::from_str(&update.pool_address) {
        Ok(p) => p,
        Err(_) => return None,
    };

    let base_mint = match Pubkey::from_str(&update.base_mint) {
        Ok(m) => m,
        Err(_) => return None,
    };

    let quote_mint = match Pubkey::from_str(&update.quote_mint) {
        Ok(m) => m,
        Err(_) => return None,
    };

    // Build minimal state based on DEX type
    let state = match update.dex.as_str() {
        "orca" => CachedPoolState::Orca(OrcaWhirlpoolState {
            token_mint_a: base_mint,
            token_mint_b: quote_mint,
            token_vault_a: Pubkey::default(), // Will be refreshed by Geyser
            token_vault_b: Pubkey::default(),
            tick_current_index: 0,
            sqrt_price: 0,
            liquidity: 0,
            fee_rate: 0,
            protocol_fee_rate: 0,
            tick_spacing: 0,
            vault_a_balance: Some(update.base_reserve),
            vault_b_balance: Some(update.quote_reserve),
            token_a_program: None,
            token_b_program: None,
        }),
        "raydium_amm" | "raydium" => CachedPoolState::RaydiumAmm(RaydiumAmmState {
            base_mint,
            quote_mint,
            coin_vault: Pubkey::default(), // Will be refreshed by Geyser
            pc_vault: Pubkey::default(),
            base_decimals: 0,
            quote_decimals: 0,
            coin_reserve: Some(update.base_reserve),
            pc_reserve: Some(update.quote_reserve),
            market_id: Pubkey::default(),
            serum_bids: None,
            serum_asks: None,
            serum_event_queue: None,
        }),
        "raydium_cpmm" => CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: base_mint,
            token_1_mint: quote_mint,
            token_0_vault: Pubkey::default(),
            token_1_vault: Pubkey::default(),
            reserve_0: Some(update.base_reserve),
            reserve_1: Some(update.quote_reserve),
        }),
        "meteora_cpmm" | "meteora_dlmm" => CachedPoolState::Meteora(MeteoraState {
            token_x_mint: base_mint,
            token_y_mint: quote_mint,
            reserve_x: Pubkey::default(),
            reserve_y: Pubkey::default(),
            active_id: 0,
            bin_step: 0,
            reserve_x_balance: Some(update.base_reserve),
            reserve_y_balance: Some(update.quote_reserve),
        }),
        "pump_amm" => CachedPoolState::PumpAmm(PumpAmmState {
            base_mint,
            quote_mint,
            pool_base_token_account: Pubkey::default(),
            pool_quote_token_account: Pubkey::default(),
            base_reserve: Some(update.base_reserve),
            quote_reserve: Some(update.quote_reserve),
            pool_accounts: Vec::new(),
        }),
        "pumpfun" => CachedPoolState::PumpFun(PumpFunState {
            token_mint: base_mint,
            bonding_curve: pool_addr,
            associated_bonding_curve: Pubkey::default(),
            virtual_sol_reserves: update.quote_reserve,
            virtual_token_reserves: update.base_reserve,
            real_sol_reserves: 0,
            real_token_reserves: 0,
            complete: false,
            creator: Pubkey::default(),
        }),
        _ => {
            debug!(dex = %update.dex, "Unsupported DEX type for minimal state");
            return None;
        }
    };

    Some((pool_addr, state))
}

/// Bootstrap LivePoolCache from JetStream (state recovery after restart)
///
/// This function pulls the last PoolCacheUpdate for each pool from JetStream,
/// giving execution-engine immediate state recovery. After bootstrap, the
/// SLAVE subscribes to incremental updates via JetStream Consumer.
///
/// # Arguments
///
/// * `nats_client` - Connected NATS client
/// * `live_pool_cache` - LivePoolCache to populate
///
/// # Returns
///
/// Number of pools recovered from JetStream
async fn bootstrap_pool_cache_from_jetstream(
    nats_client: &NatsClient,
    live_pool_cache: &ironcrab::execution::live_pool_cache::LivePoolCache,
) -> Result<usize> {
    use async_nats::jetstream;
    use futures::StreamExt;

    info!("SLAVE CACHE BOOTSTRAP: Pulling state from JetStream...");

    let jetstream = jetstream::new(nats_client.client().clone());

    // Get or create stream (idempotent)
    let stream = match jetstream.get_stream(STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, stream = STREAM_NAME, "JetStream stream not found (market-data may not be running)");
            return Ok(0);
        }
    };

    // Create ephemeral consumer with LastPerSubject deliver policy
    let consumer_config = slave_consumer_config();
    let consumer = stream.create_consumer(consumer_config).await?;

    let mut pools_recovered = 0;
    let batch_size = 1000; // Fetch up to 1000 messages per batch

    // Fetch all available messages in batches until exhausted
    loop {
        let mut messages = consumer.fetch().max_messages(batch_size).messages().await?;
        let mut batch_count = 0;

        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "Error fetching message from JetStream");
                    continue;
                }
            };

            batch_count += 1;

            // Deserialize PoolCacheUpdate
            let pool_update: PoolCacheUpdate = match serde_json::from_slice(&msg.payload) {
                Ok(u) => u,
                Err(e) => {
                    warn!(error = %e, "Failed to deserialize PoolCacheUpdate from JetStream");
                    if let Err(ack_err) = msg.ack().await {
                        warn!(error = %ack_err, "Failed to ack message");
                    }
                    continue;
                }
            };

            // Apply update to LivePoolCache
            match pool_update.update_type {
                PoolCacheUpdateType::PoolDiscovered | PoolCacheUpdateType::BalanceUpdated => {
                    if let Some((pool_addr, minimal_state)) = build_minimal_pool_state(&pool_update)
                    {
                        // Insert minimal state into LivePoolCache
                        // Full account data from Geyser will refresh this later
                        live_pool_cache.upsert(pool_addr, minimal_state, pool_update.geyser_slot);
                        pools_recovered += 1;
                    }
                }
                PoolCacheUpdateType::PoolRemoved => {
                    // Skip removed pools during bootstrap
                }
            }

            if let Err(ack_err) = msg.ack().await {
                warn!(error = %ack_err, "Failed to ack message");
            }
        }

        // If we got fewer messages than batch_size, we've exhausted the stream
        if batch_count < batch_size {
            break;
        }
    }

    info!(pools_recovered, "SLAVE CACHE BOOTSTRAP: Complete");
    Ok(pools_recovered)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("execution_engine=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
        )
        .init();

    let args = Args::parse();
    let run_id = Uuid::new_v4().to_string();

    // Optional app config: used for non-hot-path settings (e.g. Helius fallback endpoints).
    let helius_rpc_url = match AppConfig::load(&args.config) {
        Ok(c) => c.solana.helius_rpc_url,
        Err(e) => {
            warn!(
                error = %e,
                config = %args.config.display(),
                "Failed to load config TOML; Helius fallback disabled"
            );
            None
        }
    };

    info!(
        run_id = %run_id,
        config = %args.config.display(),
        simulate_only = args.simulate_only,
        dry_run = args.dry_run,
        metrics_port = args.metrics_port,
        "Starting execution-engine service"
    );

    // Start metrics server
    let metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr).await {
            error!(error = %e, "Metrics server failed");
        }
    });
    info!(
        port = args.metrics_port,
        "Metrics server started at /metrics"
    );

    // RPC wrapper (nonblocking; limiter/retry lives inside SolanaRpc)
    let rpc = Arc::new(SolanaRpc::new(&args.rpc_url));

    // RS-1.1 acceptance: prove basic RPC works through SolanaRpc
    match rpc.rpc.get_latest_blockhash().await {
        Ok(_bh) => info!("Fetched latest blockhash via SolanaRpc"),
        Err(e) => warn!(error = %e, "Failed to fetch latest blockhash via SolanaRpc"),
    }

    // === This is the ONLY binary that should load keys ===
    let treasury = match Treasury::load_from_env() {
        Ok(t) => {
            info!(wallet = %t.pubkey(), "Wallet keys loaded (execution-engine is the single signer)");
            Some(t)
        }
        Err(e) => {
            if !args.dry_run {
                warn!(error = %e, "No wallet keys configured or loadable; running without signer");
            }
            None
        }
    };
    let has_keys = treasury.is_some();

    // Load app config for Jito and other settings
    let app_config = match AppConfig::load(&args.config) {
        Ok(c) => {
            debug!("Config loaded successfully from {:?}", args.config);
            Some(c)
        }
        Err(e) => {
            warn!(
                error = %e,
                config = %args.config.display(),
                "Failed to load config for Jito settings - using defaults"
            );
            None
        }
    };

    // Jito settings from [execution_engine] section (preferred) or [sniper] section (legacy fallback)
    let exec_eng_cfg = app_config
        .as_ref()
        .and_then(|c| c.execution_engine.as_ref());
    let sniper_cfg = app_config.as_ref().and_then(|c| c.sniper.as_ref());

    // Get WSOL config from [execution_engine.wsol_manager] section
    let wsol_cfg = exec_eng_cfg.and_then(|e| e.wsol_manager.as_ref());
    // Get Janitor config from [execution_engine.account_janitor] section
    let janitor_cfg = exec_eng_cfg.and_then(|e| e.account_janitor.as_ref());
    // Get Fee Policy config from [execution_engine.fee_policy] section
    let fee_policy_cfg = exec_eng_cfg.and_then(|e| e.fee_policy.as_ref());

    // Build FeePolicy from config (or use defaults)
    let fee_policy = if let Some(fp) = fee_policy_cfg {
        FeePolicy {
            default_compute_units: fp.default_compute_units,
            max_compute_units: fp.max_compute_units,
            arb_compute_units: fp.arb_compute_units,
            default_priority_fee_micro_lamports: fp.default_priority_fee_micro_lamports,
            max_priority_fee_micro_lamports: fp.max_priority_fee_micro_lamports,
            tier0_priority_fee_micro_lamports: fp.tier0_priority_fee_micro_lamports,
            urgency_multiplier_elevated: 2.0,
            urgency_multiplier_urgent: 5.0,
            max_tx_cost_lamports: fp.max_tx_cost_lamports,
            min_profit_after_fees_bps: fp.min_profit_after_fees_bps,
        }
    } else {
        FeePolicy::default()
    };

    info!(
        tier0_priority_fee = fee_policy.tier0_priority_fee_micro_lamports,
        default_priority_fee = fee_policy.default_priority_fee_micro_lamports,
        max_priority_fee = fee_policy.max_priority_fee_micro_lamports,
        "Fee policy loaded"
    );

    // Setup config - read Jito settings: prefer [execution_engine] section, fallback to [sniper]
    let exec_config = ExecutionConfig {
        send_enabled: !args.simulate_only && !args.dry_run && has_keys,
        jito_enabled: exec_eng_cfg
            .and_then(|e| e.jito_enabled)
            .or_else(|| sniper_cfg.and_then(|s| s.jito_enabled))
            .unwrap_or(false),
        jito_tip_lamports: exec_eng_cfg
            .and_then(|e| e.jito_tip_lamports)
            .or_else(|| sniper_cfg.and_then(|s| s.jito_tip_lamports))
            .unwrap_or(10_000),
        jito_region: exec_eng_cfg
            .and_then(|e| e.jito_region.clone())
            .or_else(|| sniper_cfg.and_then(|s| s.jito_region.clone()))
            .unwrap_or_else(|| "frankfurt".to_string()),
        // WSOL Manager config (for hot-reload tracking)
        wsol_enabled: wsol_cfg.map(|c| c.enabled).unwrap_or(true),
        wsol_min_wsol_sol: wsol_cfg.map(|c| c.min_wsol_sol).unwrap_or(0.5),
        wsol_target_wsol_sol: wsol_cfg.map(|c| c.target_wsol_sol).unwrap_or(1.0),
        wsol_max_wsol_sol: wsol_cfg.map(|c| c.max_wsol_sol).unwrap_or(2.0),
        wsol_min_native_sol: wsol_cfg.map(|c| c.min_native_sol).unwrap_or(0.1),
        wsol_cooldown_secs: wsol_cfg.map(|c| c.cooldown_secs).unwrap_or(30),
        wsol_dry_run: wsol_cfg.map(|c| c.dry_run).unwrap_or(false) || args.dry_run,
        // Account Janitor config (for hot-reload tracking)
        janitor_enabled: janitor_cfg.map(|c| c.enabled).unwrap_or(false),
        janitor_close_ata_interval_secs: janitor_cfg
            .map(|c| c.close_ata_interval_secs)
            .unwrap_or(3600),
        janitor_close_ata_min_age_secs: janitor_cfg
            .map(|c| c.close_ata_min_age_secs)
            .unwrap_or(86400),
        janitor_close_ata_max_per_run: janitor_cfg.map(|c| c.close_ata_max_per_run).unwrap_or(10),
        janitor_merge_dust_enabled: janitor_cfg.map(|c| c.merge_dust_enabled).unwrap_or(false),
        janitor_merge_dust_interval_secs: janitor_cfg
            .map(|c| c.merge_dust_interval_secs)
            .unwrap_or(300),
        janitor_merge_dust_max_per_run: janitor_cfg.map(|c| c.merge_dust_max_per_run).unwrap_or(5),
        janitor_swap_dust_enabled: janitor_cfg.map(|c| c.swap_dust_enabled).unwrap_or(false),
        janitor_swap_dust_interval_secs: janitor_cfg
            .map(|c| c.swap_dust_interval_secs)
            .unwrap_or(86400),
        janitor_swap_dust_min_value_sol: janitor_cfg
            .map(|c| c.swap_dust_min_value_sol)
            .unwrap_or(0.001),
        janitor_swap_dust_max_slippage_bps: janitor_cfg
            .map(|c| c.swap_dust_max_slippage_bps)
            .unwrap_or(500),
        janitor_swap_dust_max_per_run: janitor_cfg.map(|c| c.swap_dust_max_per_run).unwrap_or(5),
        janitor_dry_run: janitor_cfg.map(|c| c.dry_run).unwrap_or(false) || args.dry_run,
        // Fee Policy
        fee_policy,
        liquidation_priority_fee_micro_lamports: fee_policy_cfg
            .and_then(|fp| fp.liquidation_priority_fee_micro_lamports),
        liquidation_max_priority_fee_micro_lamports: fee_policy_cfg
            .and_then(|fp| fp.liquidation_max_priority_fee_micro_lamports),
        liquidation_max_tx_cost_lamports: fee_policy_cfg
            .and_then(|fp| fp.liquidation_max_tx_cost_lamports),
        ..Default::default()
    };

    info!(
        jito_enabled = exec_config.jito_enabled,
        jito_region = %exec_config.jito_region,
        jito_tip = exec_config.jito_tip_lamports,
        "Jito config loaded"
    );

    if exec_config.send_enabled {
        info!("Transaction sending ENABLED");
    } else {
        let reason = if args.dry_run {
            "dry_run"
        } else if args.simulate_only {
            "simulate_only"
        } else if !has_keys {
            "no_keys"
        } else {
            "disabled"
        };
        info!(reason, "Transaction sending DISABLED");
    }

    // P1: Setup Jito client for atomic bundle execution
    // CRITICAL: Use ALL regions in parallel for lowest latency and highest success rate
    let jito_client = if exec_config.jito_enabled && !args.dry_run {
        // Use all 5 Jito regions in parallel - bundles are deduplicated by signature
        let regions = JitoRegion::all();
        let client = JitoClient::new(regions.clone(), exec_config.jito_tip_lamports);
        info!(
            regions = ?regions.iter().map(|r| r.url()).collect::<Vec<_>>(),
            tip_lamports = %exec_config.jito_tip_lamports,
            "Jito client initialized with ALL regions for parallel submission"
        );
        Some(client)
    } else {
        if exec_config.jito_enabled {
            info!("Jito configured but disabled in dry-run mode");
        } else {
            debug!("Jito bundle execution disabled");
        }
        None
    };

    // P0: Load Address Lookup Table for transaction size reduction
    // Required for cross-DEX arbitrage (transactions > 1232 bytes without ALT)
    let address_lookup_table = if let Some(alt_addr_str) =
        exec_eng_cfg.and_then(|e| e.address_lookup_table.as_ref())
    {
        match solana_sdk::pubkey::Pubkey::from_str(alt_addr_str) {
            Ok(alt_pubkey) => {
                match ironcrab::solana::address_lookup_table::load_alt(&rpc.rpc, &alt_pubkey).await
                {
                    Ok(loaded_alt) => {
                        info!(
                            alt_address = %alt_pubkey,
                            accounts_count = loaded_alt.accounts.len(),
                            "Loaded Address Lookup Table for TX size reduction"
                        );
                        Some(loaded_alt)
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            alt_address = %alt_pubkey,
                            "Failed to load ALT - transactions may fail due to size limit"
                        );
                        None
                    }
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    alt_address = alt_addr_str,
                    "Invalid ALT address in config"
                );
                None
            }
        }
    } else {
        info!("No Address Lookup Table configured - cross-DEX arb may fail due to TX size");
        None
    };

    // Setup JSONL writers
    let log_base = args
        .log_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("trade_logs"));

    let decision_config =
        JsonlWriterConfig::new("decision_records").with_log_dir(log_base.join("decisions"));
    let decision_writer = JsonlWriter::new(decision_config)?;

    let execution_config =
        JsonlWriterConfig::new("execution_results").with_log_dir(log_base.join("executions"));
    let execution_writer = JsonlWriter::new(execution_config)?;

    let burn_config = JsonlWriterConfig::new("burn_ops").with_log_dir(log_base.join("burns"));
    let burn_writer = JsonlWriter::new(burn_config)?;

    // WSOL Manager and Account Janitor writers
    let wsol_config_jsonl =
        JsonlWriterConfig::new("wsol_actions").with_log_dir(log_base.join("wsol"));
    let wsol_writer = Arc::new(JsonlWriter::new(wsol_config_jsonl)?);

    let janitor_config_jsonl =
        JsonlWriterConfig::new("janitor_actions").with_log_dir(log_base.join("janitor"));
    let janitor_writer = Arc::new(JsonlWriter::new(janitor_config_jsonl)?);

    info!(log_dir = %log_base.display(), "JSONL writers initialized");

    // Load wallet keys and fetch real balance
    // NOTE: In `--dry-run`, we still allow key loading and balance reads.
    // `--dry-run` means: do not submit transactions, not: keyless.
    let (wallet_pubkey, initial_balance) = if let Some(ref t) = treasury {
        let pubkey = t.pubkey();

        // RS-1.1 acceptance: getBalance through SolanaRpc
        match rpc.rpc.get_balance(&pubkey).await {
            Ok(balance) => {
                info!(wallet = %pubkey, balance_sol = balance as f64 / 1e9, "Real wallet balance fetched");
                (Some(pubkey), balance)
            }
            Err(e) => {
                warn!(error = %e, "Failed to fetch wallet balance, using default");
                (Some(pubkey), args.initial_sol_lamports)
            }
        }
    } else {
        (None, args.initial_sol_lamports)
    };

    // Setup lock manager with real balance
    let lock_manager = LockManager::new(initial_balance);
    info!(
        initial_sol = initial_balance,
        balance_sol = initial_balance as f64 / 1e9,
        "Lock manager initialized with wallet balance"
    );

    // Update metrics with real balance
    AVAILABLE_SOL_LAMPORTS.store(initial_balance, Ordering::Relaxed);

    // P1: Load state snapshot if available (DoD K)
    let snapshot = StateSnapshot::load(log_base.as_path());

    // Restore processed intents (idempotency)
    if let Some(ref snap) = snapshot {
        lock_manager.set_processed_intents(snap.processed_intents.clone());
        info!(
            restored_intents = snap.processed_intents.len(),
            "Idempotency store restored from snapshot"
        );
    }

    // Setup NATS
    // NOTE: `--dry-run` means "never send on-chain transactions".
    // It must NOT disable NATS consumption, otherwise we can't end-to-end test the pipeline.
    let nats = {
        let config = NatsConfig::new(&args.nats_url, "execution-engine");
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            warn!(error = %e, "Failed to connect to NATS (continuing without)");
            None
        } else {
            info!(url = %args.nats_url, "Connected to NATS");
            Some(client)
        }
    };

    // P1: Determine initial values from snapshot (DoD K)
    // Architecture: execution-engine does NOT scan wallet via RPC (that's market-data's job).
    // Positions are tracked purely from snapshot + ExecutionResults.
    // If reconciliation is needed after manual sales/transfers, use market-data's
    // WalletBalanceSnapshot events consumed by momentum-bot (strategy plane).

    let (
        initial_day,
        initial_daily_loss,
        initial_positions,
        initial_decision_counter,
        initial_execution_counter,
    ) = if let Some(ref snap) = snapshot {
        if snap.is_same_day() {
            // Same day: restore all counters
            info!(
                daily_loss = snap.daily_loss_lamports,
                open_positions = snap.open_positions,
                decision_counter = snap.decision_counter,
                "Restored same-day state from snapshot"
            );
            let positions = snap.open_positions;
            (
                chrono::NaiveDate::parse_from_str(&snap.day, "%Y-%m-%d")
                    .unwrap_or_else(|_| chrono::Utc::now().date_naive()),
                snap.daily_loss_lamports,
                positions,
                snap.decision_counter,
                snap.execution_counter,
            )
        } else {
            // New day: reset daily counters but keep counters for ID generation
            info!(
                old_day = %snap.day,
                "New day detected, resetting daily loss but keeping ID counters"
            );
            // New day: daily loss resets. Positions should be 0 if all were closed,
            // or restored from previous snapshot if holdings persist.
            let positions = 0; // Reset positions on new day (clean slate)
            (
                chrono::Utc::now().date_naive(),
                0, // Reset daily loss
                positions,
                snap.decision_counter, // Keep for unique IDs across restarts
                snap.execution_counter,
            )
        }
    } else {
        // Fresh start (no snapshot)
        (
            chrono::Utc::now().date_naive(),
            0,
            0, // No positions on fresh start
            0,
            0,
        )
    };

    // Kill-switch persistence: survives restarts and is independent of day.
    let initial_kill_switch_active = snapshot
        .as_ref()
        .map(|s| s.kill_switch_active)
        .unwrap_or(false);

    // Option C: Initialize LivePoolCache for zero-RPC quote calculation
    // This cache is fed by Geyser and used by tx_builder for fresh min_out calculations
    let live_pool_cache: Option<SharedLivePoolCache> = app_config
        .as_ref()
        .and_then(|c| c.solana.geyser_grpc_url.as_ref())
        .map(|_url| {
            info!("Initializing LivePoolCache for zero-RPC TX building");
            create_shared_cache()
        });

    // Bootstrap LivePoolCache from JetStream (state recovery after restart)
    if let (Some(ref nats_client), Some(ref cache)) = (&nats, &live_pool_cache) {
        match bootstrap_pool_cache_from_jetstream(nats_client, cache).await {
            Ok(pools_recovered) => {
                info!(
                    pools_recovered,
                    "SLAVE CACHE: State recovered from JetStream"
                );
            }
            Err(e) => {
                warn!(error = %e, "SLAVE CACHE: JetStream bootstrap failed (will rely on incremental updates)");
            }
        }
    }

    let mut ctx = ExecutionContext {
        run_id: run_id.clone(),
        rpc_url: args.rpc_url.clone(),
        helius_rpc_url,
        wallet_pubkey,
        treasury,
        config_snapshot_id: parking_lot::RwLock::new(exec_config.snapshot_id()),
        config: parking_lot::RwLock::new(exec_config),
        nats,
        decision_writer,
        execution_writer,
        burn_writer,
        lock_manager,
        log_base: log_base.clone(),
        decision_counter: std::sync::atomic::AtomicU64::new(initial_decision_counter),
        execution_counter: std::sync::atomic::AtomicU64::new(initial_execution_counter),
        // Risk tracking - restored from snapshot
        current_day: parking_lot::RwLock::new(initial_day),
        daily_loss_lamports: std::sync::atomic::AtomicI64::new(initial_daily_loss),
        open_positions: std::sync::atomic::AtomicUsize::new(initial_positions),
        kill_switch_active: AtomicBool::new(initial_kill_switch_active),
        liquidation_in_progress: AtomicBool::new(false),
        kill_switch_context: parking_lot::RwLock::new(None),
        burn_in_progress: AtomicBool::new(false),
        // P1: Jito bundle support
        jito_client,
        bundles_submitted: std::sync::atomic::AtomicU64::new(0),
        bundles_confirmed: std::sync::atomic::AtomicU64::new(0),
        // Cross-DEX handler (initialized below)
        cross_dex_handler: None,
        rpc: Arc::clone(&rpc),
        // P0: Address Lookup Table for TX size reduction
        address_lookup_table,
        // P2: TxSender with fallback chain (initialized below after ctx creation)
        tx_sender: None,
        // P2: Dynamic priority fees from Geyser (via market-data NATS)
        dynamic_fee_percentiles: parking_lot::RwLock::new(None),
        // Option C: LivePoolCache - for zero-RPC quote calculation
        live_pool_cache: live_pool_cache.clone(),
        // Metrics
        intents_received: std::sync::atomic::AtomicU64::new(0),
        intents_rejected: std::sync::atomic::AtomicU64::new(0),
        sim_failures: std::sync::atomic::AtomicU64::new(0),
        tx_sent: std::sync::atomic::AtomicU64::new(0),
        arb_validated: std::sync::atomic::AtomicU64::new(0),
        arb_executed: std::sync::atomic::AtomicU64::new(0),
    };

    // Initialize cross-DEX handler (keyless: uses treasury pubkey for user authority).
    // If this fails, we keep it disabled and cross-DEX arb intents will be rejected with
    // ARB_HANDLER_NOT_CONFIGURED.
    {
        let user_pk = ctx.treasury.as_ref().map(|t| t.pubkey());
        let mut handler =
            CrossDexHandler::new(Arc::clone(&ctx.rpc), user_pk).with_rpc_url(ctx.rpc_url.clone());

        // P0 FIX: Inject LivePoolCache for fresh Geyser-based quotes in build_swap_plan()
        // Without this, CrossDexHandler falls back to stale arb-strategy price metadata!
        if let Some(ref cache) = live_pool_cache {
            handler = handler.with_pool_cache(Arc::clone(cache));
            info!("CrossDexHandler: LivePoolCache injected for fresh Geyser quotes");
        }

        match handler.init_dexes().await {
            Ok(()) => {
                ctx.cross_dex_handler = Some(Arc::new(handler));
                info!("Initialized CrossDexHandler with pump_amm and meteora_dlmm support");
            }
            Err(e) => {
                warn!(error = %e, "Failed to initialize CrossDexHandler; cross-DEX arb disabled");
            }
        }
    }

    // === P2: TxSender with TPU/Jito/RPC fallback chain ===
    // Provides unified TX submission with automatic fallback:
    // TPU Direct (~50-100ms) → RPC (~200-400ms)
    // Note: Jito bundles are handled separately in the main intent loop for arb transactions
    {
        let tx_submission_cfg = exec_eng_cfg
            .and_then(|e| e.tx_submission.clone())
            .unwrap_or_default();

        // Derive WebSocket URL from RPC URL for TPU leader schedule
        let ws_url = ctx
            .rpc_url
            .replace("http://", "ws://")
            .replace("https://", "wss://")
            .replace(":8899", ":8900"); // Standard WS port

        // TxSender uses blocking RpcClient internally (for TpuClient compatibility)
        // Create a new blocking RpcClient instance for TxSender
        let blocking_rpc = Arc::new(solana_client::rpc_client::RpcClient::new(
            ctx.rpc_url.clone(),
        ));

        // Note: JitoClient is not passed to TxSender (arb bundles handled separately)
        // This TxSender is primarily for non-arb momentum trades using TPU → RPC fallback
        match TxSender::new(blocking_rpc, &ws_url, tx_submission_cfg.clone(), None).await {
            Ok(sender) => {
                ctx.tx_sender = Some(Arc::new(sender));
                info!(
                    primary = %tx_submission_cfg.primary_method,
                    fallback = ?tx_submission_cfg.fallback_chain,
                    tpu_enabled = tx_submission_cfg.tpu_enabled,
                    "TxSender initialized with fallback chain (TPU \u{2192} RPC)"
                );
            }
            Err(e) => {
                warn!(error = %e, "Failed to initialize TxSender, will use direct RPC fallback");
            }
        }
    }

    let ctx = Arc::new(ctx);

    // LivePoolCache is now synced via NATS from market-data (Single Source of Truth)
    // No longer spawning cache_geyser_task - execution-engine subscribes to PoolCacheUpdates instead

    // Publish initial gauge values immediately (before the first 30s heartbeat).
    OPEN_POSITIONS_GAUGE.store(ctx.get_open_positions() as u64, Ordering::Relaxed);

    // Shutdown channel for background tasks (WsolManager, etc.)
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // === WsolManager: Background WSOL balance maintenance ===
    // Professional arb bots don't wrap/unwrap in the arb TX itself
    if let Some(ref treasury) = ctx.treasury {
        // Get WsolManager config from [execution_engine.wsol_manager] section
        let wsol_config = exec_eng_cfg
            .and_then(|e| e.wsol_manager.as_ref())
            .cloned()
            .map(|cfg| WsolManagerConfig {
                enabled: cfg.enabled,
                min_wsol_sol: cfg.min_wsol_sol,
                target_wsol_sol: cfg.target_wsol_sol,
                max_wsol_sol: cfg.max_wsol_sol,
                min_native_sol: cfg.min_native_sol,
                cooldown_secs: cfg.cooldown_secs,
                dry_run: cfg.dry_run || args.dry_run,
            })
            .unwrap_or_else(|| WsolManagerConfig {
                // Defaults with dry_run from args
                dry_run: args.dry_run,
                ..Default::default()
            });

        if wsol_config.enabled {
            // Create separate NATS connection for WsolManager
            let nats_url = args.nats_url.clone();
            let ctx_for_kill_switch = Arc::clone(&ctx);
            let wsol_manager = WsolManager::with_jsonl_writer(
                wsol_config.clone(),
                Arc::new(treasury.clone()),
                Arc::clone(&ctx.rpc),
                env!("CARGO_PKG_VERSION"),
                &run_id,
                Arc::clone(&wsol_writer),
            )
            .with_kill_switch(move || ctx_for_kill_switch.is_kill_switch_active());
            let shutdown_rx_wsol = shutdown_rx.clone();

            tokio::spawn(async move {
                // Create separate NATS client for WsolManager
                let nats_config = NatsConfig::new(&nats_url, "wsol-manager");
                let mut nats_client = NatsClient::new(nats_config);
                match nats_client.connect().await {
                    Ok(()) => {
                        info!("WsolManager NATS connected");
                        if let Err(e) = wsol_manager
                            .run(Arc::new(nats_client), shutdown_rx_wsol)
                            .await
                        {
                            error!(error = %e, "WsolManager task failed");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "WsolManager failed to connect to NATS (running without events)");
                        // Run anyway - will use periodic fallback polling
                        if let Err(e) = wsol_manager.run_polling_only(shutdown_rx_wsol).await {
                            error!(error = %e, "WsolManager polling task failed");
                        }
                    }
                }
            });

            info!(
                min_wsol = wsol_config.min_wsol_sol,
                target_wsol = wsol_config.target_wsol_sol,
                max_wsol = wsol_config.max_wsol_sol,
                dry_run = wsol_config.dry_run,
                "WsolManager background task started"
            );
        } else {
            info!("WsolManager disabled by config");
        }
    } else {
        debug!("WsolManager not started (no Treasury)");
    }

    // === AccountJanitor: Background cleanup of empty ATAs and dust ===
    if let Some(ref treasury) = ctx.treasury {
        use ironcrab::execution::account_janitor::{AccountJanitor, AccountJanitorConfig};

        // Get AccountJanitor config from [execution_engine.account_janitor] section
        let janitor_config = exec_eng_cfg
            .and_then(|e| e.account_janitor.as_ref())
            .cloned()
            .map(|cfg| AccountJanitorConfig {
                enabled: cfg.enabled,
                close_ata_interval_secs: cfg.close_ata_interval_secs,
                close_ata_min_age_secs: cfg.close_ata_min_age_secs,
                close_ata_max_per_run: cfg.close_ata_max_per_run,
                merge_dust_enabled: cfg.merge_dust_enabled,
                merge_dust_interval_secs: cfg.merge_dust_interval_secs,
                merge_dust_max_per_run: cfg.merge_dust_max_per_run,
                swap_dust_enabled: cfg.swap_dust_enabled,
                swap_dust_interval_secs: cfg.swap_dust_interval_secs,
                swap_dust_min_value_sol: cfg.swap_dust_min_value_sol,
                swap_dust_max_slippage_bps: cfg.swap_dust_max_slippage_bps,
                swap_dust_max_per_run: cfg.swap_dust_max_per_run,
                dry_run: cfg.dry_run || args.dry_run,
            })
            .unwrap_or_else(|| AccountJanitorConfig {
                dry_run: args.dry_run,
                ..Default::default()
            });

        if janitor_config.enabled {
            // Build Router from CrossDexHandler's DEX connectors (if available)
            // This enables swap_dust feature to swap dust tokens to SOL
            let janitor_router = ctx.cross_dex_handler.as_ref().map(|handler| {
                let dexes = handler.get_all_dexes();
                Arc::new(Router::new(dexes))
            });

            let janitor = if let Some(router) = janitor_router {
                info!(
                    "AccountJanitor: Router available with {} DEXes for swap_dust",
                    ctx.cross_dex_handler
                        .as_ref()
                        .map(|h| h.get_all_dexes().len())
                        .unwrap_or(0)
                );
                AccountJanitor::with_router_and_jsonl(
                    janitor_config.clone(),
                    Arc::new(treasury.clone()),
                    Arc::clone(&ctx.rpc),
                    router,
                    Arc::clone(&janitor_writer),
                    run_id.clone(),
                )
            } else {
                warn!("AccountJanitor: No Router available, swap_dust disabled");
                AccountJanitor::with_jsonl_writer(
                    janitor_config.clone(),
                    Arc::new(treasury.clone()),
                    Arc::clone(&ctx.rpc),
                    Arc::clone(&janitor_writer),
                    run_id.clone(),
                )
            };
            let shutdown_rx_janitor = shutdown_rx.clone();

            tokio::spawn(async move {
                if let Err(e) = janitor.run(shutdown_rx_janitor).await {
                    error!(error = %e, "AccountJanitor task failed");
                }
            });

            info!(
                interval_secs = janitor_config.close_ata_interval_secs,
                min_age_secs = janitor_config.close_ata_min_age_secs,
                max_per_run = janitor_config.close_ata_max_per_run,
                swap_dust_enabled = janitor_config.swap_dust_enabled,
                dry_run = janitor_config.dry_run,
                "AccountJanitor background task started"
            );
        } else {
            debug!("AccountJanitor disabled by config");
        }
    } else {
        debug!("AccountJanitor not started (no Treasury)");
    }

    // === Main Loop: Process TradeIntents ===
    info!("Entering main execution loop");

    // P1 Crash Isolation: Signal systemd that we're ready
    #[cfg(unix)]
    {
        // NOTE: Do NOT unset NOTIFY_SOCKET here; we need it for Watchdog pings.
        let _ = sd_notify::notify(false, &[NotifyState::Ready]);
        debug!("Sent sd_notify READY to systemd");
    }

    // Keep readiness fresh even when idle.
    ironcrab::metrics::record_activity();

    // P1 Crash Isolation: systemd watchdog should continue to be pinged even when the
    // main loop is busy (e.g. liquidation/burn jobs can do long RPC calls).
    // This runs independently of the select-loop tick.
    #[cfg(unix)]
    {
        tokio::spawn(async {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
            }
        });
    }

    // Subscribe to TradeIntents if NATS connected
    let intent_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_TRADE_INTENTS).await {
            Ok(sub) => {
                info!(topic = TOPIC_TRADE_INTENTS, "Subscribed to TradeIntents");
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to TradeIntents");
                None
            }
        }
    } else {
        None
    };

    // P1: Subscribe to Config Updates (Runtime Configuration via UI)
    // Core NATS fallback subscription (for backward compatibility)
    let config_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONFIG_RELOAD).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_CONFIG_RELOAD,
                    "Subscribed to Config Updates (Core NATS fallback)"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to Config Updates");
                None
            }
        }
    } else {
        None
    };

    // P2: Subscribe to Dynamic Priority Fee Percentiles (from market-data via Geyser)
    // These are used instead of static config values for better fee estimation
    let priority_fee_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_PRIORITY_FEE_SAMPLES).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_PRIORITY_FEE_SAMPLES,
                    "Subscribed to Dynamic Priority Fee Percentiles"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to Priority Fee Percentiles; using static config");
                None
            }
        }
    } else {
        None
    };

    // P1: JetStream Config Consumer (persisted, solves race condition)
    let config_js_consumer = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());

        match jetstream.get_stream(CONFIG_STREAM_NAME).await {
            Ok(stream) => {
                match stream
                    .create_consumer(config_consumer_config("execution-engine"))
                    .await
                {
                    Ok(consumer) => {
                        info!(
                            stream = CONFIG_STREAM_NAME,
                            subject = %config_subject("execution-engine"),
                            "Subscribed to JetStream Config Updates (persisted)"
                        );

                        // Bootstrap: Try to get the last config from JetStream
                        match consumer.fetch().max_messages(1).messages().await {
                            Ok(mut messages) => {
                                use futures::StreamExt;
                                if let Some(Ok(msg)) = messages.next().await {
                                    match serde_json::from_slice::<ConfigUpdate>(&msg.payload) {
                                        Ok(update) => {
                                            info!(
                                                component = %update.target_component,
                                                keys = ?update.config.keys().collect::<Vec<_>>(),
                                                "Bootstrap: Applying config from JetStream"
                                            );
                                            let response = ctx.apply_config_update(&update);
                                            info!(
                                                status = ?response.status,
                                                applied = ?response.applied_keys,
                                                "Bootstrap config applied"
                                            );
                                            if let Err(e) = msg.ack().await {
                                                warn!(error = %e, "Failed to ack bootstrap config");
                                            }
                                        }
                                        Err(e) => {
                                            warn!(error = %e, "Failed to deserialize bootstrap config");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                debug!(error = %e, "No bootstrap config in JetStream (first run or empty)");
                            }
                        }

                        Some(consumer)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create JetStream config consumer");
                        None
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, stream = CONFIG_STREAM_NAME, "JetStream CONFIG_UPDATES stream not found (control-plane may not be running)");
                None
            }
        }
    } else {
        None
    };

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

    // For MVP dry-run test
    let mut simulated_tick: u64 = 0;
    let mut test_intent_processed = false;

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    // Use channels to bridge NATS subscriptions to the main select! loop
    // This prevents message loss when one branch is busy
    let (intent_tx, mut intent_rx) = tokio::sync::mpsc::channel::<TradeIntent>(100);
    let (control_tx, mut control_rx) = tokio::sync::mpsc::channel::<ControlRequest>(10);
    let (config_tx, mut config_rx) = tokio::sync::mpsc::channel::<ConfigUpdate>(10);

    // Spawn dedicated task for TradeIntents subscription
    if let Some(mut intent_sub) = intent_subscription {
        let tx = intent_tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = intent_sub.next().await {
                match msg.deserialize::<TradeIntent>() {
                    Ok(intent) => {
                        if tx.send(intent).await.is_err() {
                            warn!("TradeIntent channel closed, stopping subscription");
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to deserialize TradeIntent");
                    }
                }
            }
        });
    }

    // Spawn dedicated task for ConfigUpdate subscription (Core NATS fallback)
    if let Some(mut config_sub) = config_subscription {
        let tx = config_tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = config_sub.next().await {
                match msg.deserialize::<ConfigUpdate>() {
                    Ok(update) => {
                        if tx.send(update).await.is_err() {
                            warn!("ConfigUpdate channel closed, stopping subscription");
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to deserialize ConfigUpdate");
                    }
                }
            }
        });
    }

    // Spawn dedicated task for JetStream ConfigUpdate subscription (preferred, persistent)
    if let Some(config_consumer) = config_js_consumer {
        let tx = config_tx.clone();
        tokio::spawn(async move {
            use futures::StreamExt;
            loop {
                match config_consumer
                    .fetch()
                    .max_messages(10)
                    .expires(std::time::Duration::from_secs(5))
                    .messages()
                    .await
                {
                    Ok(mut messages) => {
                        while let Some(msg_result) = messages.next().await {
                            if let Ok(msg) = msg_result {
                                match serde_json::from_slice::<ConfigUpdate>(&msg.payload) {
                                    Ok(update) => {
                                        if tx.send(update).await.is_err() {
                                            warn!("ConfigUpdate channel closed, stopping JetStream subscription");
                                            return;
                                        }
                                        if let Err(e) = msg.ack().await {
                                            warn!(error = %e, "Failed to ack JetStream config message");
                                        }
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "Failed to deserialize JetStream ConfigUpdate");
                                        let _ = msg.ack().await;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!(error = %e, "JetStream config fetch returned (expected when no new messages)");
                    }
                }
            }
        });
    }

    // Spawn dedicated task for ControlRequests subscription
    if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONTROL_REQUESTS).await {
            Ok(mut control_sub) => {
                info!(
                    topic = TOPIC_CONTROL_REQUESTS,
                    "Subscribed to ControlRequests"
                );
                let tx = control_tx.clone();
                tokio::spawn(async move {
                    while let Some(msg) = control_sub.next().await {
                        info!("Received raw ControlRequest message from NATS");
                        match msg.deserialize::<ControlRequest>() {
                            Ok(req) => {
                                info!(target = %req.target, kind = ?req.kind, "Parsed ControlRequest, forwarding to main loop");
                                if tx.send(req).await.is_err() {
                                    warn!("ControlRequest channel closed, stopping subscription");
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to deserialize ControlRequest");
                            }
                        }
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to ControlRequests");
            }
        }
    }

    // P2: Spawn dedicated task for Priority Fee Percentiles subscription
    // Updates dynamic_fee_percentiles in ctx for use in TX building
    if let Some(mut fee_sub) = priority_fee_subscription {
        let ctx_clone = Arc::clone(&ctx);
        tokio::spawn(async move {
            while let Some(msg) = fee_sub.next().await {
                match serde_json::from_slice::<PriorityFeePercentiles>(&msg.payload) {
                    Ok(percentiles) => {
                        // Update the shared state
                        *ctx_clone.dynamic_fee_percentiles.write() = Some(percentiles.clone());
                        debug!(
                            p50 = percentiles.p50,
                            p90 = percentiles.p90,
                            tier0 = percentiles.tier0_recommended,
                            tier1 = percentiles.tier1_recommended,
                            arb = percentiles.arb_recommended,
                            samples = percentiles.sample_count,
                            "Updated dynamic priority fee percentiles from market-data"
                        );
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to deserialize PriorityFeePercentiles");
                    }
                }
            }
            warn!("Priority fee subscription ended");
        });
    }

    // Subscribe to PoolCacheUpdates from JetStream (Single Source of Truth)
    let pool_cache_consumer = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());

        match jetstream.get_stream(STREAM_NAME).await {
            Ok(stream) => {
                // Create durable consumer for live updates (picks up where bootstrap left off)
                match stream.create_consumer(slave_consumer_config()).await {
                    Ok(consumer) => {
                        info!(
                            stream = STREAM_NAME,
                            "Subscribed to JetStream PoolCacheUpdates (SLAVE cache sync from market-data MASTER)"
                        );
                        Some(consumer)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create JetStream consumer for PoolCacheUpdates");
                        None
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, stream = STREAM_NAME, "JetStream stream not found (market-data may not be running)");
                None
            }
        }
    } else {
        None
    };

    let mut pool_cache_consumer_opt = pool_cache_consumer;

    loop {
        tokio::select! {
            // Receive TradeIntents from channel (buffered from dedicated subscription task)
            Some(intent) = intent_rx.recv() => {
                info!(intent_id = %intent.intent_id, source = %intent.source, "Received TradeIntent from NATS");
                if let Err(e) = process_intent(&ctx, intent).await {
                    error!(error = %e, "Failed to process intent");
                }
            }

            // Receive Config Updates from channel
            Some(update) = config_rx.recv() => {
                // Only process if targeted at execution-engine
                if update.target_component == "execution-engine" {
                    info!(
                        component = %update.target_component,
                        keys = ?update.config.keys().collect::<Vec<_>>(),
                        "Received Config Update from control-plane"
                    );
                    let response = ctx.apply_config_update(&update);
                    info!(
                        status = ?response.status,
                        applied = ?response.applied_keys,
                        rejected = ?response.rejected_keys,
                        "Config update processed"
                    );
                } else {
                    debug!(component = %update.target_component, "Ignoring config update for other component");
                }
            }

            // Receive ControlRequests from channel (KillSwitch + liquidation)
            Some(req) = control_rx.recv() => {
                info!(target = %req.target, kind = ?req.kind, "Received ControlRequest from channel");
                if req.target != "execution-engine" && req.target != "all" {
                    debug!(target = %req.target, "Ignoring ControlRequest for other target");
                } else {
                    let request_id = req.request_id.clone();
                    match req.kind {
                        ControlRequestKind::KillSwitch { active, reason, liquidate_positions, max_slippage_bps, ttl_ms } => {
                            ctx.kill_switch_active.store(active, Ordering::Relaxed);
                            info!(active, liquidate_positions, reason = ?reason, "Kill switch updated");
                            if active {
                                ctx.set_kill_switch_context(Some(KillSwitchContext {
                                    reason: reason.clone(),
                                    source: Some(req.header.component.clone()),
                                    liquidate_positions: Some(liquidate_positions),
                                    request_id: Some(request_id.clone()),
                                }));
                            } else {
                                ctx.set_kill_switch_context(None);
                            }

                            // Persist immediately so restarts don't silently drop the kill switch.
                            if let Err(e) = ctx.save_state() {
                                warn!(error = %e, "Failed to persist state after kill switch update");
                            }

                            if active && liquidate_positions {
                                // Check if liquidation is already in progress BEFORE spawning
                                if ctx.liquidation_in_progress.load(Ordering::SeqCst) {
                                    warn!("KillSwitch: Liquidation already in progress, ignoring duplicate request");
                                } else {
                                    let slippage = max_slippage_bps.unwrap_or(9900);
                                    let ttl = ttl_ms.unwrap_or(60_000);
                                    ExecutionContext::run_liquidation_job(
                                        Arc::clone(&ctx),
                                        slippage,
                                        ttl,
                                        reason,
                                    )
                                    .await;
                                }
                            }
                        }
                        ControlRequestKind::ResetKillSwitch => {
                            ctx.kill_switch_active.store(false, Ordering::Relaxed);
                            info!("Kill switch reset");
                            ctx.set_kill_switch_context(None);

                            // Persist immediately so restarts don't re-enable a previously active kill switch.
                            if let Err(e) = ctx.save_state() {
                                warn!(error = %e, "Failed to persist state after kill switch reset");
                            }
                        }
                        ControlRequestKind::BurnTokenAccounts { owner_pubkey, token_accounts, close_accounts, reason } => {
                            ExecutionContext::run_manual_burn_job(
                                Arc::clone(&ctx),
                                request_id,
                                owner_pubkey,
                                token_accounts,
                                close_accounts,
                                reason,
                            )
                            .await;
                        }
                    }
                }
            }

            _ = interval.tick() => {
                simulated_tick += 1;

                // Keep /ready fresh even when no intents flow.
                ironcrab::metrics::record_activity();

                // Process any available PoolCacheUpdates from JetStream Consumer
                if let Some(ref mut consumer) = pool_cache_consumer_opt {
                    use futures::StreamExt;

                    // Fetch up to 100 messages per tick with 100ms timeout (NON-BLOCKING!)
                    // Without expires(), fetch() can block indefinitely waiting for messages,
                    // which freezes the entire select! loop and prevents processing intents/control requests.
                    match consumer.fetch().max_messages(100).expires(std::time::Duration::from_millis(100)).messages().await {
                        Ok(mut messages) => {
                            let mut msg_count = 0;
                            while let Some(msg_result) = messages.next().await {
                                match msg_result {
                                    Ok(msg) => {
                                        match serde_json::from_slice::<PoolCacheUpdate>(&msg.payload) {
                                            Ok(update) => {
                                                // Apply update to local LivePoolCache
                                                if let Some(ref cache) = ctx.live_pool_cache {
                                                    match update.update_type {
                                                        PoolCacheUpdateType::PoolDiscovered | PoolCacheUpdateType::BalanceUpdated => {
                                                            if let Some((pool_addr, minimal_state)) = build_minimal_pool_state(&update) {
                                                                cache.upsert(pool_addr, minimal_state, update.geyser_slot);
                                                                msg_count += 1;
                                                            }
                                                        }
                                                        PoolCacheUpdateType::PoolRemoved => {}
                                                    }
                                                }
                                                // Ack the message
                                                if let Err(e) = msg.ack().await {
                                                    warn!(error = %e, "Failed to ack JetStream message");
                                                }
                                            }
                                            Err(e) => {
                                                warn!(error = %e, "Failed to deserialize PoolCacheUpdate");
                                                if let Err(ack_err) = msg.ack().await {
                                                    warn!(error = %ack_err, "Failed to ack message");
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        debug!(error = %e, "JetStream fetch returned error");
                                    }
                                }
                            }
                            if msg_count > 0 {
                                debug!(msg_count, "SLAVE CACHE: Synced PoolCacheUpdates from JetStream");
                            }
                        }
                        Err(e) => {
                            // Expected when no new messages - don't log as warning
                            debug!(error = %e, "No new messages in JetStream");
                        }
                    }
                }

                // MVP/dev convenience: simulate receiving a test intent once when running dry-run
                // *without* NATS (so local dev still does something).
                if simulated_tick == 5 && !test_intent_processed && args.dry_run && ctx.nats.is_none() {
                    test_intent_processed = true;

                    let test_intent = create_test_intent(&ctx.run_id);
                    info!(intent_id = %test_intent.intent_id, "Processing test intent");

                    if let Err(e) = process_intent(&ctx, test_intent).await {
                        error!(error = %e, "Failed to process intent");
                    }
                }

                // Periodic cleanup and stats
                if simulated_tick % 30 == 0 {
                    ctx.lock_manager.cleanup_expired();

                    let (cap_locks, res_locks) = ctx.lock_manager.active_lock_count();
                    let received = ctx.intents_received.load(std::sync::atomic::Ordering::Relaxed);
                    let rejected = ctx.intents_rejected.load(std::sync::atomic::Ordering::Relaxed);
                    let sim_fail = ctx.sim_failures.load(std::sync::atomic::Ordering::Relaxed);
                    let available_sol = ctx.lock_manager.available_sol();

                    // Update Prometheus metrics
                    INTENTS_RECEIVED_TOTAL.store(received, Ordering::Relaxed);
                    INTENTS_REJECTED_TOTAL.store(rejected, Ordering::Relaxed);
                    SIMULATION_FAILURES_TOTAL.store(sim_fail, Ordering::Relaxed);
                    OPEN_POSITIONS_GAUGE.store(ctx.get_open_positions() as u64, Ordering::Relaxed);
                    ACTIVE_CAPITAL_LOCKS.store(cap_locks as u64, Ordering::Relaxed);
                    ACTIVE_RESOURCE_LOCKS.store(res_locks as u64, Ordering::Relaxed);
                    AVAILABLE_SOL_LAMPORTS.store(available_sol, Ordering::Relaxed);

                    info!(
                        tick = simulated_tick,
                        intents_received = received,
                        intents_rejected = rejected,
                        sim_failures = sim_fail,
                        active_capital_locks = cap_locks,
                        active_resource_locks = res_locks,
                        available_sol = available_sol,
                        "Execution-engine heartbeat"
                    );
                }

                // P1 Crash Isolation: Ping systemd watchdog frequently enough to avoid edge timing.
                if simulated_tick % 10 == 0 {
                    #[cfg(unix)]
                    let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
                }

                // P1: Periodic state save every 60 ticks (~1 minute) (DoD K)
                if simulated_tick % 60 == 0 {
                    if let Err(e) = ctx.save_state() {
                        warn!(error = %e, "Failed to save periodic state snapshot");
                    } else {
                        debug!(tick = simulated_tick, "Periodic state snapshot saved");
                    }
                }
            }
            _ = &mut shutdown => {
                info!("Shutdown signal received, stopping background tasks");
                // Signal shutdown to background tasks (WsolManager, etc.)
                let _ = shutdown_tx.send(true);
                break;
            }
        }
    }

    // P1: Save state snapshot on shutdown (DoD K)
    if let Err(e) = ctx.save_state() {
        error!(error = %e, "Failed to save state snapshot");
    }

    // Flush JSONL on shutdown
    ctx.decision_writer.flush()?;
    ctx.execution_writer.flush()?;
    info!(run_id = %run_id, "execution-engine shutdown complete");

    Ok(())
}

/// Create a test intent for MVP demonstration
fn create_test_intent(run_id: &str) -> TradeIntent {
    use ironcrab::ipc::{ExplicitAmount, IntentTier, TradeResources, TradeSide};

    TradeIntent::new(
        "test",
        BUILD_VERSION,
        run_id,
        format!("test-intent-{}", Uuid::new_v4()),
        "test-harness",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(50_000_000, 9), // 0.05 SOL
        TradeResources {
            input_mint: "So11111111111111111111111111111111111111112".to_string(),
            output_mint: "TestToken123".to_string(),
            pools: vec!["TestPool456".to_string()],
            accounts: vec![],
            token_program: None, // Test tokens use SPL Token by default
        },
        100, // 1% expected ROI
        200, // 2% max slippage
        TradeSide::Buy,
        TradingRegime::Early,
    )
    .with_ttl_ms(5000)
}

/// Process a single TradeIntent through the execution pipeline
async fn process_intent(ctx: &ExecutionContext, intent: TradeIntent) -> Result<()> {
    ctx.record_intent_received();

    // Keep Prometheus counters aligned with persisted decision/intents logs.
    // (The periodic heartbeat also stores aggregated counts; this makes the metric
    // responsive and avoids confusing under-counting after restarts.)
    INTENTS_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);

    let decision_id = ctx.next_decision_id();
    let mut checks: Vec<CheckResult> = Vec::new();

    // P1: Get config snapshot for this decision (hot-reloadable)
    let config = ctx.get_config();

    let is_liquidation_sell = intent.side == TradeSide::Sell
        && (intent
            .metadata
            .get("purpose")
            .map(|v| v == "liquidation")
            .unwrap_or(false)
            || intent
                .metadata
                .get("kill_switch")
                .map(|v| v == "true")
                .unwrap_or(false));

    info!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        source = %intent.source,
        "Processing intent"
    );
    if intent.side == TradeSide::Sell {
        if let Some(sell_routing) = intent.metadata.get("sell_routing") {
            info!(
                intent_id = %intent.intent_id,
                sell_routing = %sell_routing,
                "Sell routing path"
            );
        }
    }

    // Update received counter
    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);

    // === Check 1: Idempotency ===
    if ctx.lock_manager.is_duplicate(&intent.intent_id) {
        let reason = RejectReason::LockDuplicateIntent;
        REJECT_DUPLICATE.fetch_add(1, Ordering::Relaxed);
        checks.push(CheckResult {
            check_name: "idempotency".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some("Intent already processed".to_string()),
        });

        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "idempotency".to_string(),
        passed: true,
        reason_code: None,
        details: None,
    });

    // === Check 2: TTL validity ===
    // For MVP, assume TTL is valid (would check against current time/slot)
    checks.push(CheckResult {
        check_name: "ttl_valid".to_string(),
        passed: true,
        reason_code: None,
        details: None,
    });

    // === Risk Invariant Checks (DoD J) ===

    // Reset daily counters if new day
    ctx.maybe_reset_daily();

    // Check 3: Kill switch (BUY only)
    if intent.side == TradeSide::Buy {
        if ctx.is_kill_switch_active() {
            let reason = RejectReason::KillSwitchActive;
            checks.push(CheckResult {
                check_name: "kill_switch".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some("kill_switch_active: buy blocked".to_string()),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        checks.push(CheckResult {
            check_name: "kill_switch".to_string(),
            passed: true,
            reason_code: None,
            details: Some("inactive (buy_only)".to_string()),
        });
    } else {
        checks.push(CheckResult {
            check_name: "kill_switch".to_string(),
            passed: true,
            reason_code: None,
            details: Some("skipped_for_sell".to_string()),
        });
    }

    // Check 3a: Max position size (applies to BUY only)
    if intent.side == TradeSide::Buy {
        if intent.required_capital.raw > config.max_position_size_lamports {
            let reason = RejectReason::RiskMaxPosition;
            checks.push(CheckResult {
                check_name: "max_position_size".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "required={} > max={}",
                    intent.required_capital.raw, config.max_position_size_lamports
                )),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        checks.push(CheckResult {
            check_name: "max_position_size".to_string(),
            passed: true,
            reason_code: None,
            details: Some(format!(
                "required={} <= max={}",
                intent.required_capital.raw, config.max_position_size_lamports
            )),
        });
    } else {
        checks.push(CheckResult {
            check_name: "max_position_size".to_string(),
            passed: true,
            reason_code: None,
            details: Some("skipped_for_sell".to_string()),
        });
    }

    // Check 3b: Max slippage
    if is_liquidation_sell {
        checks.push(CheckResult {
            check_name: "max_slippage".to_string(),
            passed: true,
            reason_code: None,
            details: Some("skipped_for_liquidation_sell".to_string()),
        });
    } else {
        if intent.max_slippage_bps > config.max_slippage_bps {
            let reason = RejectReason::SimSlippageExceeded;
            checks.push(CheckResult {
                check_name: "max_slippage".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "intent_slippage={}bps > max={}bps",
                    intent.max_slippage_bps, config.max_slippage_bps
                )),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        checks.push(CheckResult {
            check_name: "max_slippage".to_string(),
            passed: true,
            reason_code: None,
            details: None,
        });
    }

    // Check 3c: Max open positions (applies to BUY only; SELL exits should remain possible)
    if intent.side == TradeSide::Buy {
        let current_positions = ctx.get_open_positions();
        if current_positions >= config.max_open_positions {
            let reason = RejectReason::RiskMaxOpenPositions;
            checks.push(CheckResult {
                check_name: "max_open_positions".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "current={} >= max={}",
                    current_positions, config.max_open_positions
                )),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        checks.push(CheckResult {
            check_name: "max_open_positions".to_string(),
            passed: true,
            reason_code: None,
            details: Some("current < max (buy_only)".to_string()),
        });
    } else {
        checks.push(CheckResult {
            check_name: "max_open_positions".to_string(),
            passed: true,
            reason_code: None,
            details: Some("skipped_for_sell".to_string()),
        });
    }

    // Check 3d: Daily loss limit (applies to BUY only; SELL exits should remain possible)
    if intent.side == TradeSide::Buy {
        let daily_loss = ctx.get_daily_loss_lamports();
        if daily_loss >= config.daily_loss_limit_lamports as i64 {
            let reason = RejectReason::RiskDailyLossLimit;
            checks.push(CheckResult {
                check_name: "daily_loss_limit".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "daily_loss={} >= limit={}",
                    daily_loss, config.daily_loss_limit_lamports
                )),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        checks.push(CheckResult {
            check_name: "daily_loss_limit".to_string(),
            passed: true,
            reason_code: None,
            details: Some("ok (buy_only)".to_string()),
        });
    } else {
        checks.push(CheckResult {
            check_name: "daily_loss_limit".to_string(),
            passed: true,
            reason_code: None,
            details: Some("skipped_for_sell".to_string()),
        });
    }

    // Check 3e: SELL token balance preflight (avoid emitting SELL intents we cannot fulfill)
    if intent.side == TradeSide::Sell {
        let wallet = match ctx.wallet_pubkey {
            Some(pk) => pk,
            None => {
                let reason = RejectReason::InternalError;
                checks.push(CheckResult {
                    check_name: "sell_token_balance".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some("wallet_pubkey_unavailable (no keys loaded)".to_string()),
                });
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
        };

        let rpc = Arc::clone(&ctx.rpc);

        let mint_str = intent.resources.input_mint.clone();
        let required_raw = intent.required_capital.raw;

        let balance_check: Result<(u64, Pubkey, Pubkey)> = async {
            let mint = Pubkey::from_str(&mint_str)
                .map_err(|_| anyhow::anyhow!("invalid input_mint: {}", mint_str))?;

            let spl_token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
            let token_2022_program =
                Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
                    .expect("static pubkey");

            // GEYSER-FIRST: Get token program from cache, not RPC
            let cached_program = if let Some(cache) = ctx.live_pool_cache.as_ref() {
                cache.get_mint_program(&mint)
            } else {
                None
            };

            // If we have a cached program, use it directly
            if let Some(token_program) = cached_program {
                let ata_spl = get_associated_token_address_with_program_id(
                    &sdk_to_spl(&wallet),
                    &sdk_to_spl(&mint),
                    &sdk_to_spl(&token_program),
                );
                let ata = spl_to_sdk(&ata_spl);

                let available_raw = match rpc.rpc.get_token_account_balance(&ata).await {
                    Ok(ui) => ui
                        .amount
                        .parse::<u64>()
                        .map_err(|e| anyhow::anyhow!("invalid token amount: {}", e))?,
                    Err(_) => 0,
                };

                return Ok((available_raw, ata, token_program));
            }

            // No cache hit: Try SPL Token first (most common), then Token-2022
            let ata_spl = get_associated_token_address_with_program_id(
                &sdk_to_spl(&wallet),
                &sdk_to_spl(&mint),
                &sdk_to_spl(&spl_token_program),
            );
            let ata_spl_sdk = spl_to_sdk(&ata_spl);

            let spl_balance = match rpc.rpc.get_token_account_balance(&ata_spl_sdk).await {
                Ok(ui) => ui.amount.parse::<u64>().unwrap_or(0),
                Err(_) => 0,
            };

            // If SPL Token has balance, use it
            if spl_balance > 0 {
                return Ok((spl_balance, ata_spl_sdk, spl_token_program));
            }

            // Try Token-2022
            let ata_2022 = get_associated_token_address_with_program_id(
                &sdk_to_spl(&wallet),
                &sdk_to_spl(&mint),
                &sdk_to_spl(&token_2022_program),
            );
            let ata_2022_sdk = spl_to_sdk(&ata_2022);

            let t2022_balance = match rpc.rpc.get_token_account_balance(&ata_2022_sdk).await {
                Ok(ui) => ui.amount.parse::<u64>().unwrap_or(0),
                Err(_) => 0,
            };

            if t2022_balance > 0 {
                return Ok((t2022_balance, ata_2022_sdk, token_2022_program));
            }

            // Neither has balance, return SPL Token ATA with 0
            Ok((0, ata_spl_sdk, spl_token_program))
        }
        .await;

        match balance_check {
            Ok((available_raw, ata, _token_program)) => {
                // Seed lock-manager token availability so we can lock against it.
                // This prevents overlapping SELLs on the same mint from overbooking.
                ctx.lock_manager.set_available_token_balance(
                    intent.resources.input_mint.clone(),
                    available_raw,
                );

                if available_raw < required_raw {
                    let reason = RejectReason::SimInsufficientBalance;
                    checks.push(CheckResult {
                        check_name: "sell_token_balance".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(format!(
                            "available={} < required={} (mint={}, ata={})",
                            available_raw, required_raw, intent.resources.input_mint, ata
                        )),
                    });
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }

                checks.push(CheckResult {
                    check_name: "sell_token_balance".to_string(),
                    passed: true,
                    reason_code: None,
                    details: Some(format!(
                        "available={} >= required={} (ata={})",
                        available_raw, required_raw, ata
                    )),
                });
            }
            Err(e) => {
                let reason = RejectReason::InternalError;
                checks.push(CheckResult {
                    check_name: "sell_token_balance".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some(format!("rpc_error: {}", e)),
                });
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
        }
    }

    // === P1: Fee/Compute Policy Checks ===
    let mut effective_fee_policy = config.fee_policy.clone();
    let fee_policy_label = if is_liquidation_sell {
        if let Some(v) = config.liquidation_priority_fee_micro_lamports {
            effective_fee_policy.tier0_priority_fee_micro_lamports = v;
        }
        if let Some(v) = config.liquidation_max_priority_fee_micro_lamports {
            effective_fee_policy.max_priority_fee_micro_lamports = v;
        }
        if let Some(v) = config.liquidation_max_tx_cost_lamports {
            effective_fee_policy.max_tx_cost_lamports = v;
        }
        "liquidation"
    } else {
        "standard"
    };
    if is_liquidation_sell {
        info!(
            intent_id = %intent.intent_id,
            fee_policy = %fee_policy_label,
            "Using liquidation fee policy"
        );
    }

    // Check: Compute units within limit
    let compute_units = effective_fee_policy.compute_units_for_intent(&intent);
    if compute_units > effective_fee_policy.max_compute_units {
        let reason = RejectReason::FeeComputeExceedsLimit;
        checks.push(CheckResult {
            check_name: "fee_compute_limit".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "compute_units={} > max={}",
                compute_units, effective_fee_policy.max_compute_units
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "fee_compute_limit".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!("compute_units={}", compute_units)),
    });

    // Check: Priority fee within limit (P2: use dynamic fees if available)
    let priority_fee = ctx.get_priority_fee_for_intent(&intent, &effective_fee_policy);
    if priority_fee > effective_fee_policy.max_priority_fee_micro_lamports {
        let reason = RejectReason::FeePriorityExceedsLimit;
        checks.push(CheckResult {
            check_name: "fee_priority_limit".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "priority_fee={} > max={}",
                priority_fee, effective_fee_policy.max_priority_fee_micro_lamports
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "fee_priority_limit".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!("priority_fee_micro_lamports={}", priority_fee)),
    });

    // Check: Total transaction cost within limit
    let (base_fee, priority_fee_lamports, total_cost) =
        effective_fee_policy.estimate_tx_cost(&intent);
    if total_cost > effective_fee_policy.max_tx_cost_lamports {
        let reason = RejectReason::FeeExceedsMaxCost;
        checks.push(CheckResult {
            check_name: "fee_max_cost".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "total_cost={} (base={}, priority={}) > max={}",
                total_cost, base_fee, priority_fee_lamports, effective_fee_policy.max_tx_cost_lamports
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "fee_max_cost".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "total_cost={} (base={}, priority={})",
            total_cost, base_fee, priority_fee_lamports
        )),
    });

    // Check: Trade profitable after fees (ARB only)
    // For momentum-bot BUY intents, we don't know the profit yet - it's speculative.
    // For arb-strategy, the expected_roi_bps is known upfront (spread between DEXes).
    // For SELL exits (incl. liquidations), skip - exits aren't expected to be profitable-after-fees.
    let is_arb_intent = intent.source == "arb-strategy";
    if intent.side == TradeSide::Buy && is_arb_intent {
        let (is_profitable, profit_after_fees_bps) =
            effective_fee_policy.is_profitable_after_fees(&intent);
        if !is_profitable {
            let reason = RejectReason::FeeUnprofitable;
            checks.push(CheckResult {
                check_name: "fee_profitability".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "profit_after_fees={}bps < min={}bps",
                    profit_after_fees_bps, effective_fee_policy.min_profit_after_fees_bps
                )),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        checks.push(CheckResult {
            check_name: "fee_profitability".to_string(),
            passed: true,
            reason_code: None,
        details: Some(format!(
            "profit_after_fees={}bps >= min={}bps",
            profit_after_fees_bps, effective_fee_policy.min_profit_after_fees_bps
        )),
        });
    } else {
        // Skip for momentum-bot (speculative), SELL exits, and other non-arb sources
        let skip_reason = if intent.side != TradeSide::Buy {
            "skipped_for_sell"
        } else {
            "skipped_for_speculative_buy"
        };
        checks.push(CheckResult {
            check_name: "fee_profitability".to_string(),
            passed: true,
            reason_code: None,
            details: Some(skip_reason.to_string()),
        });
    }

    // === Check 4: Resource locks (pools + mints) ===
    let holder = LockHolder::new(&intent.intent_id)
        .with_decision(&decision_id)
        .with_tier(intent.tier as u8)
        .with_source(&intent.source); // P1: Source for fairness tracking

    let mut locked_resources = 0u64;
    for pool in &intent.resources.pools {
        match ctx
            .lock_manager
            .try_lock_resource(holder.clone(), pool, ResourceType::Pool)
        {
            LockResult::Acquired | LockResult::AcquiredByPreemption { .. } => {
                locked_resources += 1;
            }
            LockResult::Conflict { holder: existing } => {
                let reason = RejectReason::LockResourceConflict;
                REJECT_RESOURCE_LOCK.fetch_add(1, Ordering::Relaxed);
                checks.push(CheckResult {
                    check_name: "resource_lock".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some(format!("pool locked by {}", existing.intent_id)),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
            LockResult::InsufficientCapital { .. } => {
                // Not applicable for resource locks
            }
        }
    }

    for mint in [&intent.resources.input_mint, &intent.resources.output_mint] {
        if mint.is_empty() {
            continue;
        }
        match ctx
            .lock_manager
            .try_lock_resource(holder.clone(), mint, ResourceType::Mint)
        {
            LockResult::Acquired | LockResult::AcquiredByPreemption { .. } => {
                locked_resources += 1;
            }
            LockResult::Conflict { holder: existing } => {
                let reason = RejectReason::LockResourceConflict;
                REJECT_RESOURCE_LOCK.fetch_add(1, Ordering::Relaxed);
                checks.push(CheckResult {
                    check_name: "resource_lock".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some(format!("mint locked by {}", existing.intent_id)),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
            LockResult::InsufficientCapital { .. } => {
                // Not applicable for resource locks
            }
        }
    }

    checks.push(CheckResult {
        check_name: "resource_locks".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!("locked={}", locked_resources)),
    });

    // === Check 5: Capital lock (BUY: SOL, SELL: tokens) ===
    let lock_result = if intent.side == TradeSide::Buy {
        ctx.lock_manager.try_lock_capital(
            holder,
            intent.required_capital.raw,
            std::collections::HashMap::new(),
        )
    } else {
        let mut tokens = std::collections::HashMap::new();
        tokens.insert(
            intent.resources.input_mint.clone(),
            intent.required_capital.raw,
        );
        ctx.lock_manager.try_lock_capital(holder, 0, tokens)
    };

    match lock_result {
        LockResult::Acquired => {
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: true,
                reason_code: None,
                details: Some(if intent.side == TradeSide::Buy {
                    "sol".to_string()
                } else {
                    format!("token:{}", intent.resources.input_mint)
                }),
            });
        }
        LockResult::AcquiredByPreemption { preempted } => {
            // DoD L) P0: Higher-priority intent preempted lower-priority lock
            info!(
                intent_id = %intent.intent_id,
                preempted_intent = %preempted.intent_id,
                "Capital lock acquired by preemption (DoD L)"
            );
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: true,
                reason_code: None,
                details: Some(format!("Preempted: {}", preempted.intent_id)),
            });
        }
        LockResult::InsufficientCapital {
            available,
            requested,
        } => {
            let reason = RejectReason::LockCapitalConflict;
            REJECT_CAPITAL_LOCK.fetch_add(1, Ordering::Relaxed);
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "Insufficient capital: available={}, requested={}",
                    available, requested
                )),
            });
            ctx.lock_manager.release_locks(&intent.intent_id);
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        LockResult::Conflict { holder } => {
            let reason = RejectReason::LockCapitalConflict;
            REJECT_CAPITAL_LOCK.fetch_add(1, Ordering::Relaxed);
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!("Lock held by: {}", holder.intent_id)),
            });
            ctx.lock_manager.release_locks(&intent.intent_id);
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
    }

    // === Cross-DEX Arbitrage Detection (if applicable) ===
    let is_cross_dex_arb = CrossDexHandler::is_cross_dex_arb_intent(&intent);

    // Planned tx (RS-2.1): deterministic tx plan + plan_hash
    // NOTE: Cross-DEX arb intents are NOT single-swap plans and therefore must not go through
    // tx_builder::build_tx_plan (which requires metadata.dex and pools_len==1).
    let (tx_plan, plan_hash_str) = {
        info!(intent_id = %intent.intent_id, "Building tx plan");

        let wallet_pubkey = match ctx.wallet_pubkey {
            Some(pk) => pk,
            None => {
                let reason = RejectReason::InternalError;
                checks.push(CheckResult {
                    check_name: "tx_plan".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some("wallet_pubkey_unavailable (keys not loaded)".to_string()),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
        };

        if is_cross_dex_arb {
            info!(intent_id = %intent.intent_id, "Planning cross-DEX arb tx (atomic bundle)");

            let Some(ref handler) = ctx.cross_dex_handler else {
                let reason = RejectReason::ArbHandlerNotConfigured;
                checks.push(CheckResult {
                    check_name: "cross_dex_handler".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some("Cross-DEX handler not initialized".to_string()),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            };

            // Use the same fee policy estimates that will be used for the actual send path.
            let (_base_fee, _priority_fee_lamports, total_cost_lamports) =
                effective_fee_policy.estimate_tx_cost(&intent);

            // Cross-DEX arb must be revalidated with live quotes before plan is built.
            let validation = match handler
                .validate_arb_opportunity(&intent, total_cost_lamports)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "Cross-DEX validation failed");
                    let reason = RejectReason::ArbValidationError;
                    checks.push(CheckResult {
                        check_name: "cross_dex_validation".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(e.to_string()),
                    });
                    ctx.lock_manager.release_locks(&intent.intent_id);
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }
            };

            ctx.arb_validated
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            if !validation.is_valid {
                let reason = RejectReason::ArbSpreadInsufficient;
                checks.push(CheckResult {
                    check_name: "cross_dex_validation".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: validation.reject_reason.clone(),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }

            checks.push(CheckResult {
                check_name: "cross_dex_validation".to_string(),
                passed: true,
                reason_code: None,
                details: Some(format!(
                    "spread={}bps profit={}lamports tx_cost={}lamports",
                    validation.actual_spread_bps,
                    validation.estimated_profit_lamports,
                    total_cost_lamports
                )),
            });

            // Build the two-leg swap instruction plan.
            let plan = match handler.build_swap_plan(&intent, &validation).await {
                Ok(p) => p,
                Err(e) => {
                    let reason = RejectReason::UnsupportedIntent;
                    checks.push(CheckResult {
                        check_name: "tx_plan".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(format!("cross_dex_plan_error:{e}")),
                    });
                    ctx.lock_manager.release_locks(&intent.intent_id);
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }
            };

            // Include compute budget ixs so simulation matches send (and CU limit is sufficient).
            // P2: Use dynamic priority fees if available from Geyser
            let compute_units = effective_fee_policy.compute_units_for_intent(&intent);
            let micro_lamports_per_cu =
                ctx.get_priority_fee_for_intent(&intent, &effective_fee_policy);

            let mut ixs = Vec::new();
            ixs.push(
                ironcrab::solana::compute_budget_helper::set_compute_unit_limit(compute_units),
            );
            if micro_lamports_per_cu > 0 {
                ixs.push(
                    ironcrab::solana::compute_budget_helper::set_compute_unit_price(
                        micro_lamports_per_cu,
                    ),
                );
            }
            ixs.extend(plan.buy_instructions);
            ixs.extend(plan.sell_instructions);

            let tx_plan = tx_builder::TxPlan { instructions: ixs };
            let plan_hash_str = tx_plan.hash_string();
            checks.push(CheckResult {
                check_name: "tx_plan".to_string(),
                passed: true,
                reason_code: None,
                details: Some(format!(
                    "ix_count={} plan_hash={} buy_dex={} sell_dex={}",
                    tx_plan.instructions.len(),
                    plan_hash_str,
                    plan.buy_dex,
                    plan.sell_dex
                )),
            });

            (tx_plan, plan_hash_str)
        } else {
            // Option C: Pass LivePoolCache for zero-RPC quote calculation
            match tx_builder::build_tx_plan(
                &intent,
                wallet_pubkey,
                Arc::clone(&ctx.rpc),
                ctx.live_pool_cache.as_ref(),
            )
            .await
            {
                tx_builder::TxPlanOutcome::Planned(plan) => {
                    let plan_hash_str = plan.hash_string();
                    checks.push(CheckResult {
                        check_name: "tx_plan".to_string(),
                        passed: true,
                        reason_code: None,
                        details: Some(format!(
                            "ix_count={} plan_hash={}",
                            plan.instructions.len(),
                            plan_hash_str
                        )),
                    });
                    (plan, plan_hash_str)
                }
                tx_builder::TxPlanOutcome::Unsupported(u) => {
                    checks.push(CheckResult {
                        check_name: "tx_plan".to_string(),
                        passed: false,
                        reason_code: Some(u.reason.to_string()),
                        details: Some(u.details),
                    });
                    ctx.lock_manager.release_locks(&intent.intent_id);
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, u.reason)
                        .await;
                }
            }
        }
    };

    let plan_hash: Option<String> = Some(plan_hash_str.clone());

    // === P1: Check if bundle required for atomic execution ===
    let requires_bundle = intent.requires_bundle();

    // Debug: Log bundle requirement and send_enabled status
    info!(
        intent_id = %intent.intent_id,
        requires_bundle = %requires_bundle,
        send_enabled = %config.send_enabled,
        jito_configured = %ctx.jito_client.is_some(),
        bundle_tip_in_intent = ?intent.bundle_tip_lamports,
        "Bundle requirement check"
    );

    let wallet_pubkey = ctx
        .wallet_pubkey
        .expect("wallet_pubkey must be present after successful planning");

    if requires_bundle && config.send_enabled && ctx.jito_client.is_none() {
        // Intent requires bundle but Jito not configured
        let reason = RejectReason::BundleNotConfigured;
        checks.push(CheckResult {
            check_name: "bundle_config".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some("Intent requires atomic bundle but Jito not configured".to_string()),
        });
        ctx.lock_manager.release_locks(&intent.intent_id);
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }

    // If bundle execution is required, include the tip instruction in both simulation and send.
    // This preserves simulate-gated correctness (we simulate exactly what we will send).
    let mut bundle_tip_ix: Option<solana_sdk::instruction::Instruction> = None;
    let mut bundle_tip_lamports: Option<u64> = None;
    if requires_bundle && config.send_enabled {
        let tip_lamports = intent
            .bundle_tip_lamports
            .unwrap_or(config.jito_tip_lamports);
        info!(
            intent_id = %intent.intent_id,
            tip_lamports = %tip_lamports,
            "Building Jito tip instruction for bundle"
        );
        let jito_client = ctx
            .jito_client
            .as_ref()
            .expect("bundle_config gate ensures jito_client is present");

        match jito_client.build_tip_instruction(&wallet_pubkey, tip_lamports) {
            Ok(ix) => {
                info!(
                    intent_id = %intent.intent_id,
                    tip_lamports = %tip_lamports,
                    tip_account = %ix.accounts[1].pubkey,
                    "✅ Tip instruction built successfully"
                );
                bundle_tip_ix = Some(ix);
                bundle_tip_lamports = Some(tip_lamports);
            }
            Err(e) => {
                let reason = RejectReason::InternalError;
                checks.push(CheckResult {
                    check_name: "bundle_tip_ix".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some(format!("failed to build tip instruction: {e}")),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
        }
    } else {
        info!(
            intent_id = %intent.intent_id,
            requires_bundle = %requires_bundle,
            send_enabled = %config.send_enabled,
            "⚠️ NOT building tip instruction (condition not met)"
        );
    }

    let tx_plan_for_sim = if let Some(ref ix) = bundle_tip_ix {
        let mut ixs = tx_plan.instructions.clone();
        ixs.push(ix.clone());
        tx_builder::TxPlan { instructions: ixs }
    } else {
        tx_plan.clone()
    };

    // === Simulate (P0: simulate-gated) ===
    info!(intent_id = %intent.intent_id, "Running simulation");

    let sim_result = simulate_transaction(ctx, wallet_pubkey, &tx_plan_for_sim).await;

    if !sim_result.success {
        let reason = RejectReason::SimFailed;
        checks.push(CheckResult {
            check_name: "simulation".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: sim_result.error_code.clone(),
        });

        // Release lock on failure
        ctx.lock_manager.release_locks(&intent.intent_id);

        return emit_sim_failed_decision(
            ctx,
            decision_id,
            &intent,
            checks,
            plan_hash_str,
            sim_result,
        )
        .await;
    }

    checks.push(CheckResult {
        check_name: "simulation".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "CU consumed: {:?}",
            sim_result.compute_units_consumed
        )),
    });

    // Track bundle result for decision record
    let mut bundle_id: Option<String> = None;
    let mut send_signature: Option<String> = None;
    let mut send_method_used: Option<String> = None;
    let mut sent_anything = false;
    let mut send_failed = false;

    // === Send (if enabled) ===
    if config.send_enabled {
        if requires_bundle {
            TX_SEND_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);

            let treasury = match ctx.treasury.as_ref() {
                Some(t) => t,
                None => {
                    let reason = RejectReason::InternalError;
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some("no_signer_configured".to_string()),
                    });
                    ctx.record_intent_rejected();
                    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    ctx.lock_manager.release_locks(&intent.intent_id);
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }
            };

            let jito_client = ctx
                .jito_client
                .as_ref()
                .expect("bundle_config gate ensures jito_client is present");

            let signer: &dyn Signer = treasury.signer_ref();
            let blockhash = match ctx.rpc.get_latest_blockhash_retry().await {
                Ok(bh) => bh,
                Err(e) => {
                    let reason = RejectReason::BundleFailed;
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(format!("rpc_error:{e}")),
                    });
                    ctx.record_intent_rejected();
                    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!(intent_id = %intent.intent_id, error = %e, "Failed to fetch blockhash for bundle send");
                    // Allow retry
                    ctx.lock_manager.release_locks(&intent.intent_id);
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }
            };

            // CRITICAL: Jito bundles REQUIRE tip instruction
            // If no tip instruction present, reject intent immediately
            let tip_ix = match bundle_tip_ix {
                Some(ref ix) => ix,
                None => {
                    let reason = RejectReason::InternalError;
                    checks.push(CheckResult {
                        check_name: "bundle_tip_required".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(
                            "Jito bundle requires tip instruction but none present".to_string(),
                        ),
                    });
                    ctx.record_intent_rejected();
                    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    ctx.lock_manager.release_locks(&intent.intent_id);
                    warn!(
                        intent_id = %intent.intent_id,
                        "❌ Bundle requires tip instruction but none present - rejecting"
                    );
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }
            };

            let mut ixs = tx_plan.instructions.clone();
            ixs.push(tip_ix.clone());
            info!(
                intent_id = %intent.intent_id,
                original_ix_count = %tx_plan.instructions.len(),
                final_ix_count = %ixs.len(),
                tip_program = %tip_ix.program_id,
                tip_account = %tip_ix.accounts[1].pubkey,
                "✅ Tip instruction added to Jito bundle transaction"
            );

            // Use ALT if available to reduce transaction size (Jito bundles can exceed 1232 byte limit without ALT)
            // CRITICAL: Jito tip account MUST be writable in the final transaction.
            // If the tip account is in the ALT, v0::Message::try_compile will reference it via lookup,
            // but the v0 Message format cannot specify writable flags for lookup accounts.
            // This causes Jito rejection: "Bundles must write lock at least one tip account".
            // Solution: Filter tip account out of ALT before compiling v0 Message.
            let send_result = if let Some(ref alt) = ctx.address_lookup_table {
                // ALT path enabled for bundles to reduce TX size
                // Build versioned transaction with ALT
                // AddressLookupTableAccount, v0, VersionedMessage already imported at top
                use solana_sdk::transaction::VersionedTransaction;

                // Get the tip account from the tip instruction (accounts[1] is the recipient, accounts[0] is payer)
                let tip_account = tip_ix.accounts[1].pubkey;

                // Filter out Jito tip account from ALT to preserve its writable flag
                let original_count = alt.accounts.len();
                let filtered_accounts: Vec<Pubkey> = alt
                    .accounts
                    .iter()
                    .filter(|&addr| *addr != tip_account)
                    .copied()
                    .collect();

                if filtered_accounts.len() < original_count {
                    info!(
                        intent_id = %intent.intent_id,
                        tip_account = %tip_account,
                        removed_count = %(original_count - filtered_accounts.len()),
                        "Removed Jito tip account from ALT to preserve writable flag"
                    );
                }

                // Convert LoadedAlt to AddressLookupTableAccount for v0::Message::try_compile
                let alt_account = AddressLookupTableAccount {
                    key: alt.address,
                    addresses: filtered_accounts,
                };

                match v0::Message::try_compile(&wallet_pubkey, &ixs, &[alt_account], blockhash) {
                    Ok(v0_message) => {
                        let versioned_msg = VersionedMessage::V0(v0_message);
                        match VersionedTransaction::try_new(versioned_msg, &[signer]) {
                            Ok(versioned_tx) => {
                                info!(
                                    intent_id = %intent.intent_id,
                                    alt_address = %alt.address,
                                    alt_accounts = alt.accounts.len(),
                                    "Submitting versioned transaction with ALT to Jito"
                                );
                                jito_client.send_versioned_bundle(&[versioned_tx]).await
                            }
                            Err(e) => {
                                warn!(error = %e, "VersionedTransaction signing failed, using legacy");
                                let tx = Transaction::new_signed_with_payer(
                                    &ixs,
                                    Some(&wallet_pubkey),
                                    &[signer],
                                    blockhash,
                                );
                                jito_client.send_bundle(&[tx]).await
                            }
                        }
                    }
                    Err(e) => {
                        // Fallback to legacy if ALT compile fails
                        warn!(error = %e, "ALT message compile failed, using legacy transaction");
                        let tx = Transaction::new_signed_with_payer(
                            &ixs,
                            Some(&wallet_pubkey),
                            &[signer],
                            blockhash,
                        );
                        jito_client.send_bundle(&[tx]).await
                    }
                }
            } else {
                // Legacy transaction (fallback when no ALT available)
                info!(
                    intent_id = %intent.intent_id,
                    "Using legacy transaction for Jito bundle (no ALT available)"
                );

                // Build transaction manually to ensure proper serialization for Jito
                use solana_sdk::message::Message;
                let message = Message::new(&ixs, Some(&wallet_pubkey));
                let mut tx = Transaction::new_unsigned(message);
                tx.sign(&[signer], blockhash);

                info!(
                    intent_id = %intent.intent_id,
                    is_signed = tx.is_signed(),
                    signature_count = tx.signatures.len(),
                    message_size = bincode::serialize(&tx).map(|b| b.len()).unwrap_or(0),
                    "Legacy transaction signed for Jito bundle"
                );

                jito_client.send_bundle(&[tx]).await
            };

            if let Some(tip_lamports) = bundle_tip_lamports {
                JITO_TIP_LAMPORTS_TOTAL.fetch_add(tip_lamports, Ordering::Relaxed);
            }
            JITO_BUNDLES_SUBMITTED_TOTAL.fetch_add(1, Ordering::Relaxed);

            match send_result {
                Ok(bid) => {
                    TX_SEND_SUCCESS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    TX_SEND_JITO_TOTAL.fetch_add(1, Ordering::Relaxed);
                    sent_anything = true;
                    bundle_id = Some(bid.clone());
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: true,
                        reason_code: None,
                        details: Some(format!(
                            "bundle_id={bid} tip_lamports={}",
                            bundle_tip_lamports.unwrap_or(config.jito_tip_lamports)
                        )),
                    });
                    info!(intent_id = %intent.intent_id, bundle_id = %bid, "Bundle submitted via Jito");
                }
                Err(e) => {
                    send_failed = true;
                    let reason = RejectReason::BundleFailed;
                    let err_msg = format!("{e:?}");
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(err_msg.clone()),
                    });

                    // Reject: atomic guarantee cannot be met.
                    ctx.record_intent_rejected();
                    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    JITO_BUNDLES_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!(intent_id = %intent.intent_id, error = %err_msg, "Jito bundle submission failed");
                }
            }
        } else {
            // P2: Use TxSender with fallback chain (TPU → Jito → RPC)
            // For non-bundle transactions, this provides lower latency via TPU Direct
            match send_transaction_with_fallback(ctx, wallet_pubkey, &tx_plan, false).await {
                Ok(result) => {
                    TX_SEND_SUCCESS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    // Track send method for Grafana breakdown
                    match result.method.as_str() {
                        "tpu" => TX_SEND_TPU_TOTAL.fetch_add(1, Ordering::Relaxed),
                        "jito" => TX_SEND_JITO_TOTAL.fetch_add(1, Ordering::Relaxed),
                        _ => TX_SEND_RPC_TOTAL.fetch_add(1, Ordering::Relaxed),
                    };
                    sent_anything = true;
                    send_signature = Some(result.signature.clone());
                    send_method_used = Some(result.method.clone());
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: true,
                        reason_code: None,
                        details: Some(format!(
                            "signature={} method={}",
                            result.signature, result.method
                        )),
                    });
                    info!(
                        intent_id = %intent.intent_id,
                        signature = %result.signature,
                        method = %result.method,
                        "Transaction submitted via TxSender"
                    );
                }
                Err(err_msg) => {
                    send_failed = true;
                    let reason = RejectReason::SendFailed;
                    REJECT_SEND_FAILED.fetch_add(1, Ordering::Relaxed);
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(err_msg.clone()),
                    });

                    // Reject: do NOT claim Sent without a real signature.
                    ctx.record_intent_rejected();
                    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!(intent_id = %intent.intent_id, error = %err_msg, "sendTransaction failed");
                }
            }
        }
    } else {
        debug!(intent_id = %intent.intent_id, "Transaction sending disabled");
    }

    // Mark as processed
    // If sendTransaction failed, do NOT mark processed (allow retry).
    if !(config.send_enabled && send_failed) {
        ctx.lock_manager.mark_processed(&intent.intent_id);
    }

    // Build SendResult if we sent something
    let mut send_result = if sent_anything && (bundle_id.is_some() || send_signature.is_some()) {
        Some(ironcrab::ipc::SendResult {
            signature: send_signature.clone(),
            bundle_id: bundle_id.clone(),
            sent_at_ms: chrono::Utc::now().timestamp_millis() as u64,
            send_method: send_method_used.or_else(|| {
                Some(if bundle_id.is_some() {
                    "jito".into()
                } else {
                    "rpc".into()
                })
            }),
        })
    } else {
        None
    };

    // === Confirm (RS-4.2 / RS-7.4) ===
    // - RPC path (non-bundle): confirm signature via getSignatureStatuses.
    // - Bundle path: wait for bundle landing via Jito block engine.
    let mut final_outcome = if config.send_enabled && sent_anything {
        DecisionOutcome::Sent
    } else {
        DecisionOutcome::Rejected
    };

    if config.send_enabled && sent_anything {
        if requires_bundle {
            if let Some(ref mut sr) = send_result {
                if let Some(ref bid) = sr.bundle_id {
                    let jito_client = ctx
                        .jito_client
                        .as_ref()
                        .expect("bundle_config gate ensures jito_client is present");

                    match jito_client
                        .wait_for_bundle(bid, config.jito_timeout_secs)
                        .await
                    {
                        Ok(status) => {
                            // Bundle landed successfully
                            JITO_BUNDLES_LANDED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            TX_CONFIRMED_TOTAL.fetch_add(1, Ordering::Relaxed);

                            // If Jito returned tx signatures, record the first as a convenience.
                            if let Some(sig0) = status.transactions.first().cloned() {
                                sr.signature = Some(sig0.clone());
                            }

                            checks.push(CheckResult {
                                check_name: "confirm".to_string(),
                                passed: true,
                                reason_code: None,
                                details: Some(format!(
                                    "bundle_id={bid} slot={} confirmation_status={} txs={}",
                                    status.slot,
                                    status.confirmation_status,
                                    status.transactions.len()
                                )),
                            });
                            final_outcome = DecisionOutcome::Confirmed;
                        }
                        Err(e) => {
                            let error_msg = format!("{e:?}");
                            let is_timeout =
                                error_msg.contains("timeout") || error_msg.contains("Timeout");

                            if is_timeout {
                                JITO_BUNDLES_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
                                TX_CONFIRM_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
                                checks.push(CheckResult {
                                    check_name: "confirm".to_string(),
                                    passed: false,
                                    reason_code: Some(RejectReason::BundleTimeout.to_string()),
                                    details: Some(error_msg),
                                });
                                final_outcome = DecisionOutcome::Sent;
                            } else {
                                JITO_BUNDLES_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                checks.push(CheckResult {
                                    check_name: "confirm".to_string(),
                                    passed: false,
                                    reason_code: Some(RejectReason::BundleFailed.to_string()),
                                    details: Some(error_msg),
                                });
                                final_outcome = DecisionOutcome::FailedConfirmed;
                            }
                        }
                    }
                }
            }
        } else if let Some(ref sr) = send_result {
            if let Some(ref sig_str) = sr.signature {
                match confirm_signature_status(ctx, sig_str, config.confirmation_timeout_ms).await {
                    Ok(ConfirmOutcome::Confirmed { details }) => {
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: true,
                            reason_code: None,
                            details: Some(details),
                        });
                        final_outcome = DecisionOutcome::Confirmed;
                    }
                    Ok(ConfirmOutcome::FailedConfirmed { details }) => {
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: false,
                            reason_code: Some("confirmed_err".to_string()),
                            details: Some(details),
                        });
                        final_outcome = DecisionOutcome::FailedConfirmed;
                    }
                    Ok(ConfirmOutcome::TimeoutSent { details }) => {
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: false,
                            reason_code: Some("confirm_timeout".to_string()),
                            details: Some(details),
                        });
                        final_outcome = DecisionOutcome::Sent;
                    }
                    Err(e) => {
                        // Ambiguous confirmation: keep outcome at Sent, but record details.
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: false,
                            reason_code: Some("confirm_rpc_error".to_string()),
                            details: Some(e),
                        });
                        final_outcome = DecisionOutcome::Sent;
                    }
                }
            }
        }
    }

    let mut input_snapshots = build_input_snapshots(&intent);
    input_snapshots.insert("fee_policy".to_string(), fee_policy_label.to_string());

    // Emit decision record
    let decision = if config.send_enabled && sent_anything {
        DecisionRecord {
            header: ironcrab::ipc::RecordHeader::new(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
            ),
            decision_id: decision_id.clone(),
            intent_id: intent.intent_id.clone(),
            source: intent.source.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: None,
            kill_switch: None,
            plan_hash,
            simulate: Some(sim_result),
            send: send_result.clone(),
            outcome: final_outcome,
            config_snapshot_id: None,
            input_snapshots: input_snapshots.clone(),
        }
    } else if config.send_enabled && send_failed {
        DecisionRecord {
            header: ironcrab::ipc::RecordHeader::new(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
            ),
            decision_id: decision_id.clone(),
            intent_id: intent.intent_id.clone(),
            source: intent.source.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some(RejectReason::SendFailed.to_string()),
            kill_switch: None,
            plan_hash,
            simulate: Some(sim_result),
            send: None,
            outcome: DecisionOutcome::Rejected,
            config_snapshot_id: None,
            input_snapshots: input_snapshots.clone(),
        }
    } else if config.send_enabled {
        // Simulation succeeded but execution is not implemented.
        // Persist as a rejection so dashboards/explorers are not misleading.
        DecisionRecord {
            header: ironcrab::ipc::RecordHeader::new(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
            ),
            decision_id: decision_id.clone(),
            intent_id: intent.intent_id.clone(),
            source: intent.source.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some("send_not_implemented".to_string()),
            kill_switch: None,
            plan_hash,
            simulate: Some(sim_result),
            send: None,
            outcome: DecisionOutcome::Rejected,
            config_snapshot_id: None,
            input_snapshots: input_snapshots.clone(),
        }
    } else {
        // Don't mark this as a simulation failure: simulation succeeded, but sending is disabled.
        // Persist a clear reason for post-mortem debugging.
        let mut checks = checks;
        checks.push(CheckResult {
            check_name: "send_enabled".to_string(),
            passed: false,
            reason_code: Some("send_disabled".to_string()),
            details: Some("execution-engine config.send_enabled=false".to_string()),
        });

        // This is a policy rejection, not a sim failure.
        ctx.record_intent_rejected();
        INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);

        DecisionRecord {
            header: ironcrab::ipc::RecordHeader::new(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
            ),
            decision_id: decision_id.clone(),
            intent_id: intent.intent_id.clone(),
            source: intent.source.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some("send_disabled".to_string()),
            kill_switch: None,
            plan_hash,
            simulate: Some(sim_result),
            send: None,
            outcome: DecisionOutcome::Rejected,
            config_snapshot_id: None,
            input_snapshots,
        }
    };

    ctx.decision_writer.write(&decision)?;

    if let Some(ref nats) = ctx.nats {
        nats.publish(TOPIC_DECISION_RECORDS, &decision).await?;
    }

    // Emit an ExecutionResult so strategy-plane components (e.g. momentum-bot) can
    // manage positions and exits (stop-loss / take-profit) based on confirmed outcomes.
    if config.send_enabled {
        let status = match decision.outcome {
            DecisionOutcome::Confirmed => ExecutionStatus::Confirmed,
            DecisionOutcome::FailedConfirmed => ExecutionStatus::Failed,
            DecisionOutcome::Sent => ExecutionStatus::Sent,
            // These outcomes imply there was no successful on-chain confirmation.
            // Whether we emit is controlled by `should_emit` below.
            DecisionOutcome::Rejected | DecisionOutcome::SimFailed => ExecutionStatus::Failed,
            DecisionOutcome::Expired => ExecutionStatus::Timeout,
        };

        let should_emit = sent_anything || send_failed;
        if should_emit {
            let exec_id = ctx.next_execution_id();

            let (signature, bundle_id) = if let Some(ref sr) = send_result {
                (sr.signature.clone(), sr.bundle_id.clone())
            } else {
                (None, bundle_id.clone())
            };

            // Extract token_mint from intent (BUY: output_mint, SELL: input_mint)
            let token_mint = match intent.side {
                TradeSide::Buy => Some(intent.resources.output_mint.clone()),
                TradeSide::Sell => Some(intent.resources.input_mint.clone()),
            };

            let mut exec = ExecutionResult::new_sent(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
                exec_id,
                decision_id.clone(),
                intent.intent_id.clone(),
                intent.source.clone(),
                token_mint,
                signature,
                bundle_id,
            );

            // Best-effort fill accounting: attach fills only when we have a signature and wallet.
            // This is used downstream for correct position accounting/exit sizing.
            if matches!(status, ExecutionStatus::Confirmed) {
                if let (Some(wallet), Some(sig_str)) =
                    (ctx.wallet_pubkey, exec.signature.as_deref())
                {
                    if let Ok(sig) = Signature::from_str(sig_str) {
                        let (fill_in, fill_out, fill_status, fill_reason, wallet_sol_delta) =
                            compute_intent_fills_best_effort(ctx, wallet, &sig, &intent).await;
                        exec = exec
                            .with_fills(fill_in, fill_out)
                            .with_fill_diagnostics(fill_status, fill_reason);
                        if let Some(delta) = wallet_sol_delta {
                            exec = exec.with_sol_delta(delta);
                        }
                    }
                }
            }

            exec.status = status;
            if matches!(exec.status, ExecutionStatus::Failed) {
                exec.error_message = Some("execution_failed".to_string());
            }

            ctx.execution_writer.write(&exec)?;
            if let Some(ref nats) = ctx.nats {
                nats.publish(TOPIC_EXECUTION_RESULTS, &exec).await?;
            }
        }
    }

    // Dashboard alignment: count executions only when we have a confirmed on-chain outcome.
    if matches!(decision.outcome, DecisionOutcome::Confirmed) {
        INTENTS_EXECUTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        match intent.side {
            TradeSide::Buy => ctx.increment_open_positions(),
            TradeSide::Sell => ctx.decrement_open_positions(),
        }
        OPEN_POSITIONS_GAUGE.store(ctx.get_open_positions() as u64, Ordering::Relaxed);

        // Best-effort recent trade record for Grafana (/trades via Infinity datasource).
        // NOTE: If fill accounting is available, use it; otherwise fall back to placeholders.
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;

        let tx_hash = send_result
            .as_ref()
            .and_then(|sr| sr.signature.clone())
            .or_else(|| send_signature.clone())
            .unwrap_or_default();

        let (fill_in, fill_out, _fill_status, _fill_reason, _wallet_sol_delta) =
            if let (Some(wallet), Ok(sig)) = (ctx.wallet_pubkey, Signature::from_str(&tx_hash)) {
                compute_intent_fills_best_effort(ctx, wallet, &sig, &intent).await
            } else {
                (None, None, FillStatus::Unavailable, None, None)
            };

        let (mint, action, amount_tokens, price_sol) = match intent.side {
            TradeSide::Buy => {
                let sol_ui_fallback = intent.required_capital.as_f64();
                let sol_ui = fill_in
                    .as_ref()
                    .map(|a| a.as_f64())
                    .unwrap_or(sol_ui_fallback);
                let tok_ui = fill_out.as_ref().map(|a| a.as_f64()).unwrap_or(0.0);
                (
                    intent.resources.output_mint.clone(),
                    "BUY".to_string(),
                    tok_ui,
                    sol_ui,
                )
            }
            TradeSide::Sell => {
                let tok_ui_fallback = intent.required_capital.as_f64();
                let tok_ui = fill_in
                    .as_ref()
                    .map(|a| a.as_f64())
                    .unwrap_or(tok_ui_fallback);
                let sol_ui = fill_out.as_ref().map(|a| a.as_f64()).unwrap_or(0.0);
                (
                    intent.resources.input_mint.clone(),
                    "SELL".to_string(),
                    tok_ui,
                    sol_ui,
                )
            }
        };

        record_recent_trade(RecentTrade {
            timestamp_ms: now_ms,
            mint,
            action,
            tx_hash,
            amount_tokens,
            price_sol,
            pnl_sol: None,
            pnl_pct: None,
            latency_ms: None,
        });
    }

    // Release lock (in production: would release after confirmation)
    ctx.lock_manager.release_locks(&intent.intent_id);

    info!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        outcome = ?decision.outcome,
        "Intent processed"
    );

    Ok(())
}

/// Emit a rejected decision record
async fn emit_rejected_decision(
    ctx: &ExecutionContext,
    decision_id: String,
    intent: &TradeIntent,
    checks: Vec<CheckResult>,
    reason: RejectReason,
) -> Result<()> {
    ctx.record_intent_rejected();

    // Keep Prometheus counters aligned with decision records.
    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);

    let mut decision = DecisionRecord::new_rejected(
        "execution-engine",
        BUILD_VERSION,
        &ctx.run_id,
        decision_id.clone(),
        intent.intent_id.clone(),
        intent.source.clone(),
        intent.origin_type,
        intent.regime,
        checks,
        reason.to_string(),
    );
    if let Some(sell_routing) = intent.metadata.get("sell_routing") {
        decision = decision.with_input_snapshot("sell_routing".to_string(), sell_routing.clone());
    }
    let is_liquidation_sell = intent.side == TradeSide::Sell
        && (intent
            .metadata
            .get("purpose")
            .map(|v| v == "liquidation")
            .unwrap_or(false)
            || intent
                .metadata
                .get("kill_switch")
                .map(|v| v == "true")
                .unwrap_or(false));
    let fee_policy_label = if is_liquidation_sell {
        "liquidation"
    } else {
        "standard"
    };
    decision = decision.with_input_snapshot("fee_policy".to_string(), fee_policy_label.to_string());
    if reason == RejectReason::KillSwitchActive {
        decision.kill_switch = ctx.get_kill_switch_context();
    }

    ctx.decision_writer.write(&decision)?;

    if let Some(ref nats) = ctx.nats {
        nats.publish(TOPIC_DECISION_RECORDS, &decision).await?;
    }

    warn!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        reason = %reason,
        "Intent rejected"
    );

    Ok(())
}

/// Emit a sim-failed decision record
async fn emit_sim_failed_decision(
    ctx: &ExecutionContext,
    decision_id: String,
    intent: &TradeIntent,
    checks: Vec<CheckResult>,
    plan_hash: String,
    sim_result: SimulationResult,
) -> Result<()> {
    // Simulation failures are rejections and should show up both in totals and by-reason.
    ctx.record_sim_failure();
    ctx.record_intent_rejected();
    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
    REJECT_SIMULATION_FAIL.fetch_add(1, Ordering::Relaxed);

    let mut decision = DecisionRecord::new_sim_failed(
        "execution-engine",
        BUILD_VERSION,
        &ctx.run_id,
        decision_id.clone(),
        intent.intent_id.clone(),
        intent.source.clone(),
        intent.origin_type,
        intent.regime,
        checks,
        plan_hash,
        sim_result,
    );
    if let Some(sell_routing) = intent.metadata.get("sell_routing") {
        decision = decision.with_input_snapshot("sell_routing".to_string(), sell_routing.clone());
    }
    let is_liquidation_sell = intent.side == TradeSide::Sell
        && (intent
            .metadata
            .get("purpose")
            .map(|v| v == "liquidation")
            .unwrap_or(false)
            || intent
                .metadata
                .get("kill_switch")
                .map(|v| v == "true")
                .unwrap_or(false));
    let fee_policy_label = if is_liquidation_sell {
        "liquidation"
    } else {
        "standard"
    };
    decision = decision.with_input_snapshot("fee_policy".to_string(), fee_policy_label.to_string());

    ctx.decision_writer.write(&decision)?;

    if let Some(ref nats) = ctx.nats {
        nats.publish(TOPIC_DECISION_RECORDS, &decision).await?;
    }

    warn!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        "Intent simulation failed"
    );

    Ok(())
}

/// Real RPC simulation (RS-3.1).
///
/// Notes:
/// - Uses `sig_verify=false` (unsigned tx is fine for simulation).
/// - Uses `replace_recent_blockhash=true` so simulation does not depend on blockhash freshness.
/// - Uses Versioned Transaction (v0) with Address Lookup Table if configured.
async fn simulate_transaction(
    ctx: &ExecutionContext,
    wallet_pubkey: Pubkey,
    plan: &tx_builder::TxPlan,
) -> SimulationResult {
    use solana_sdk::transaction::VersionedTransaction;

    // Build Versioned Transaction with ALT if available
    let tx_result: Result<VersionedTransaction, String> =
        if let Some(ref alt) = ctx.address_lookup_table {
            // Use v0 message with ALT for size reduction
            let alt_account = AddressLookupTableAccount {
                key: alt.address,
                addresses: alt.accounts.clone(),
            };

            // Get a recent blockhash for v0 message compilation
            let blockhash = match ctx.rpc.rpc.get_latest_blockhash().await {
                Ok(bh) => bh,
                Err(e) => {
                    return SimulationResult {
                        success: false,
                        error_code: Some(format!("rpc_error:blockhash:{e}")),
                        logs_preview: None,
                        compute_units_consumed: None,
                    };
                }
            };

            match v0::Message::try_compile(
                &wallet_pubkey,
                &plan.instructions,
                &[alt_account],
                blockhash,
            ) {
                Ok(message) => {
                    let versioned_message = VersionedMessage::V0(message);
                    // Create unsigned versioned transaction
                    Ok(VersionedTransaction {
                        signatures: vec![solana_sdk::signature::Signature::default()],
                        message: versioned_message,
                    })
                }
                Err(e) => Err(format!("v0_compile_error:{e}")),
            }
        } else {
            // Fallback to legacy transaction (will fail if too large)
            let blockhash = match ctx.rpc.rpc.get_latest_blockhash().await {
                Ok(bh) => bh,
                Err(e) => {
                    return SimulationResult {
                        success: false,
                        error_code: Some(format!("rpc_error:blockhash:{e}")),
                        logs_preview: None,
                        compute_units_consumed: None,
                    };
                }
            };
            let message = solana_sdk::message::Message::new_with_blockhash(
                &plan.instructions,
                Some(&wallet_pubkey),
                &blockhash,
            );
            Ok(VersionedTransaction::from(Transaction::new_unsigned(
                message,
            )))
        };

    let tx = match tx_result {
        Ok(tx) => tx,
        Err(e) => {
            return SimulationResult {
                success: false,
                error_code: Some(e),
                logs_preview: None,
                compute_units_consumed: None,
            };
        }
    };

    let cfg = RpcSimulateTransactionConfig {
        sig_verify: false,
        replace_recent_blockhash: true,
        ..RpcSimulateTransactionConfig::default()
    };

    match ctx.rpc.rpc.simulate_transaction_with_config(&tx, cfg).await {
        Ok(res) => {
            let value = res.value;

            let logs_preview = value.logs.as_ref().map(|lines| {
                // Keep this small: decision records should be lightweight.
                let mut s = lines.join("\n");
                const MAX: usize = 8_000;
                if s.len() > MAX {
                    s.truncate(MAX);
                }
                s
            });

            match value.err {
                None => SimulationResult {
                    success: true,
                    error_code: None,
                    logs_preview,
                    compute_units_consumed: value.units_consumed,
                },
                Some(err) => SimulationResult {
                    success: false,
                    error_code: Some(format!("{err:?}")),
                    logs_preview,
                    compute_units_consumed: value.units_consumed,
                },
            }
        }
        Err(e) => SimulationResult {
            success: false,
            error_code: Some(format!("rpc_error:{e}")),
            logs_preview: None,
            compute_units_consumed: None,
        },
    }
}

/// Real RPC send (RS-4.1).
///
/// Notes:
/// - Only called after successful simulation (simulate-gated).
/// - Builds and SIGNS using the single-signer `Treasury`.
/// - Uses `skip_preflight=true` (we already simulated).
/// - Uses Versioned Transaction (v0) with Address Lookup Table if configured.
async fn send_transaction_rpc(
    ctx: &ExecutionContext,
    wallet_pubkey: Pubkey,
    plan: &tx_builder::TxPlan,
    skip_preflight: bool,
    preflight_commitment: Option<CommitmentLevel>,
) -> std::result::Result<String, String> {
    use solana_sdk::message::{v0, VersionedMessage};
    use solana_sdk::transaction::VersionedTransaction;

    TX_SEND_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    let treasury = ctx
        .treasury
        .as_ref()
        .ok_or_else(|| "no_signer_configured".to_string())?;

    let signer: &dyn Signer = treasury.signer_ref();
    let blockhash = ctx
        .rpc
        .get_latest_blockhash_retry()
        .await
        .map_err(|e| format!("rpc_error:{e}"))?;

    // Build Versioned Transaction with ALT if available
    let tx: VersionedTransaction = if let Some(ref alt) = ctx.address_lookup_table {
        // Use v0 message with ALT for size reduction
        let alt_account = AddressLookupTableAccount {
            key: alt.address,
            addresses: alt.accounts.clone(),
        };

        let message = v0::Message::try_compile(
            &wallet_pubkey,
            &plan.instructions,
            &[alt_account],
            blockhash,
        )
        .map_err(|e| format!("v0_compile_error:{e}"))?;

        let versioned_message = VersionedMessage::V0(message);
        VersionedTransaction::try_new(versioned_message, &[signer])
            .map_err(|e| format!("v0_sign_error:{e}"))?
    } else {
        // Fallback to legacy transaction
        let legacy_tx = Transaction::new_signed_with_payer(
            &plan.instructions,
            Some(&wallet_pubkey),
            &[signer],
            blockhash,
        );
        VersionedTransaction::from(legacy_tx)
    };

    let config = RpcSendTransactionConfig {
        skip_preflight,
        preflight_commitment,
        encoding: Some(solana_transaction_status::UiTransactionEncoding::Base64),
        max_retries: None,
        min_context_slot: None,
    };

    ctx.rpc
        .rpc
        .send_transaction_with_config(&tx, config)
        .await
        .map(|sig| sig.to_string())
        .map_err(|e| format!("rpc_error:{e}"))
}

/// Send transaction result with method tracking
struct SendTxResult {
    signature: String,
    method: String, // "tpu", "jito", "rpc"
}

/// Send transaction with TxSender fallback chain (TPU → Jito → RPC).
///
/// If TxSender is available and the transaction is NOT bundle-required,
/// this uses the configured fallback chain. Otherwise falls back to
/// direct RPC send.
///
/// This is the P2 upgrade path for lower-latency TX submission.
async fn send_transaction_with_fallback(
    ctx: &ExecutionContext,
    wallet_pubkey: Pubkey,
    plan: &tx_builder::TxPlan,
    require_bundle: bool,
) -> std::result::Result<SendTxResult, String> {
    TX_SEND_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);

    let treasury = ctx
        .treasury
        .as_ref()
        .ok_or_else(|| "no_signer_configured".to_string())?;

    let signer: &dyn Signer = treasury.signer_ref();
    let blockhash = ctx
        .rpc
        .get_latest_blockhash_retry()
        .await
        .map_err(|e| format!("rpc_error:{e}"))?;

    // Build legacy Transaction for TxSender (TxSender handles signing internally for TPU)
    // Note: We sign here because TxSender.send_with_fallback() expects a signed Transaction
    let tx = Transaction::new_signed_with_payer(
        &plan.instructions,
        Some(&wallet_pubkey),
        &[signer],
        blockhash,
    );

    // If TxSender is available, use it for the fallback chain
    if let Some(ref tx_sender) = ctx.tx_sender {
        match tx_sender.send_with_fallback(&tx, require_bundle).await {
            Ok(result) => {
                let method_str = result.method.to_string();
                info!(
                    signature = %result.signature,
                    method = %method_str,
                    bundle_id = ?result.bundle_id,
                    "TX sent via TxSender"
                );
                return Ok(SendTxResult {
                    signature: result.signature.to_string(),
                    method: method_str,
                });
            }
            Err(e) => {
                warn!(error = %e, "TxSender failed, falling back to direct RPC");
                // Fall through to direct RPC below
            }
        }
    }

    // Fallback: Direct RPC send (original behavior)
    let config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: None,
        encoding: Some(solana_transaction_status::UiTransactionEncoding::Base64),
        max_retries: None,
        min_context_slot: None,
    };

    ctx.rpc
        .rpc
        .send_transaction_with_config(
            &solana_sdk::transaction::VersionedTransaction::from(tx),
            config,
        )
        .await
        .map(|sig| SendTxResult {
            signature: sig.to_string(),
            method: "rpc".into(),
        })
        .map_err(|e| format!("rpc_error:{e}"))
}

fn parse_commitment_level_opt(value: Option<&str>) -> Option<CommitmentLevel> {
    let v = value?.trim();
    if v.is_empty() {
        return None;
    }
    match v.to_ascii_lowercase().as_str() {
        "processed" => Some(CommitmentLevel::Processed),
        "confirmed" => Some(CommitmentLevel::Confirmed),
        "finalized" => Some(CommitmentLevel::Finalized),
        _ => None,
    }
}

fn build_input_snapshots(intent: &TradeIntent) -> std::collections::HashMap<String, String> {
    let mut snapshots = std::collections::HashMap::new();
    if let Some(sell_routing) = intent.metadata.get("sell_routing") {
        snapshots.insert("sell_routing".to_string(), sell_routing.clone());
    }
    snapshots
}

enum ConfirmOutcome {
    Confirmed { details: String },
    FailedConfirmed { details: String },
    TimeoutSent { details: String },
}

/// Confirmation polling (RS-4.2).
///
/// Maps outcome per roadmap:
/// - confirmed => Confirmed
/// - status err => FailedConfirmed
/// - timeout/ambiguous => Sent (TimeoutSent)
async fn confirm_signature_status(
    ctx: &ExecutionContext,
    signature_base58: &str,
    timeout_ms: u64,
) -> std::result::Result<ConfirmOutcome, String> {
    let signature =
        Signature::from_str(signature_base58).map_err(|e| format!("invalid_signature:{e}"))?;

    let start = std::time::Instant::now();
    let deadline = Duration::from_millis(timeout_ms.max(1));
    let mut attempt: u32 = 0;

    loop {
        if start.elapsed() >= deadline {
            TX_CONFIRM_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Ok(ConfirmOutcome::TimeoutSent {
                details: format!(
                    "timeout_ms={timeout_ms} elapsed_ms={} signature={signature_base58}",
                    start.elapsed().as_millis()
                ),
            });
        }

        // Poll signature status
        let res = ctx
            .rpc
            .rpc
            .get_signature_statuses(&[signature])
            .await
            .map_err(|e| format!("rpc_error:{e}"))?;

        let status_opt = res.value.first().cloned().unwrap_or(None);

        if let Some(st) = status_opt {
            if let Some(err) = st.err {
                return Ok(ConfirmOutcome::FailedConfirmed {
                    details: format!(
                        "err={err:?} confirmations={:?} confirmation_status={:?} elapsed_ms={}",
                        st.confirmations,
                        st.confirmation_status,
                        start.elapsed().as_millis()
                    ),
                });
            }

            // Treat Confirmed or Finalized as confirmed.
            // Some RPCs return confirmations=None when rooted/finalized.
            let is_confirmed = match st.confirmation_status {
                Some(solana_transaction_status::TransactionConfirmationStatus::Confirmed)
                | Some(solana_transaction_status::TransactionConfirmationStatus::Finalized) => true,
                Some(solana_transaction_status::TransactionConfirmationStatus::Processed) => false,
                None => st.confirmations.is_none(),
            };

            if is_confirmed {
                TX_CONFIRMED_TOTAL.fetch_add(1, Ordering::Relaxed);
                return Ok(ConfirmOutcome::Confirmed {
                    details: format!(
                        "confirmations={:?} confirmation_status={:?} elapsed_ms={}",
                        st.confirmations,
                        st.confirmation_status,
                        start.elapsed().as_millis()
                    ),
                });
            }
        }

        // Backoff: small, bounded.
        attempt = attempt.saturating_add(1);
        let sleep_ms = (50u64 * attempt.min(20) as u64).min(1_000);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}

#[cfg(test)]
mod execution_engine_tests {
    use super::{select_best_route, RouteCandidate};

    #[test]
    fn select_best_route_prefers_highest_and_keeps_first_on_tie() {
        let candidates = vec![
            RouteCandidate {
                dex: "raydium".to_string(),
                amount_out: 100,
                pool_id: "pool-1".to_string(),
                accounts: vec!["a1".to_string()],
                creator: None,
            },
            RouteCandidate {
                dex: "orca".to_string(),
                amount_out: 200,
                pool_id: "pool-2".to_string(),
                accounts: vec!["a2".to_string()],
                creator: None,
            },
            RouteCandidate {
                dex: "pump_amm".to_string(),
                amount_out: 200,
                pool_id: "pool-3".to_string(),
                accounts: vec!["a3".to_string()],
                creator: None,
            },
        ];

        let best = select_best_route(candidates).expect("expected a best route");
        assert_eq!(best.dex, "orca");
        assert_eq!(best.pool_id, "pool-2");
        assert_eq!(best.amount_out, 200);
    }
}
