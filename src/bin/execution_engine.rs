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
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_message::{v0, AddressLookupTableAccount, VersionedMessage};
use solana_sdk::bs58;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::{
    hash::Hash,
    signature::Signer,
    transaction::{Transaction, VersionedTransaction},
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding, UiTransactionTokenBalance,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use ironcrab::config::Config as AppConfig;
// cache_geyser removed - execution-engine now subscribes to PoolCacheUpdates from market-data via NATS
use ironcrab::execution::error_detection::is_6005_bonding_curve_complete;
use ironcrab::execution::live_pool_cache::{
    create_shared_cache, CachedPoolState, LivePoolCache, PumpAmmState, SharedLivePoolCache,
};
use ironcrab::execution::quote_calculator;
use ironcrab::execution::tx_builder;
use ironcrab::execution::wsol_manager::{
    PendingWrapState, WsolManager, WsolManagerConfig, WSOL_MINT,
};
use ironcrab::ipc::{
    CheckResult, ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, DecisionOutcome,
    DecisionRecord, DexPoolReadiness, ExecutionResult, ExecutionStatus, ExplicitAmount,
    FairnessPolicy, FeePolicy, FillStatus, FillUnavailableReason, IntentOrigin, IntentTier,
    KillSwitchContext, MarketEvent, MarketEventKind, PoolCacheUpdate,
    PriorityFeePercentiles, RecordHeader, RejectReason, SimulationResult,
    TradeExecutionConstraints, TradeIntent, TradeResources, TradeSide, TradingRegime,
};
use ironcrab::ipc::{ControlRequest, ControlRequestKind, ControlResponse, ControlResponseStatus};
use ironcrab::metrics::{
    dec_execution_intent_rx_queue_depth, inc_execution_intent_rx_queue_depth,
    inc_execution_pool_cache_messages_processed, record_execution_engine_interval_tick_duration_ms,
    record_execution_intent_channel_wait_ms, record_execution_intent_jetstream_to_channel_ms,
    record_execution_process_intent_us, record_execution_slot_lag_at_send_slots,
    record_liquidation_seed_skipped_authority_total, record_recent_trade,
    record_tx_confirmed_slot_delta_slots, record_tx_priority_fee_source, record_tx_rebroadcast,
    record_tx_rebroadcast_during_confirm_ms, record_tx_rebroadcast_method,
    record_tx_send_to_confirm_ms, record_tx_slot_to_send_ms, serve_metrics,
    set_execution_pool_cache_consumer_pending, set_execution_wallet_snapshot_consumer_pending,
    set_readiness_control_response_sub_active, set_readiness_control_sub_active,
    set_readiness_mode, set_readiness_nats_connected, set_readiness_state_paths_initialized,
    try_record_execution_intent_header_to_receive_ms, try_record_execution_intent_to_confirm_ms,
    update_readiness_execution_engine_current, wall_clock_unix_ms_now, MetricsComponent,
    RecentTrade, ACTIVE_CAPITAL_LOCKS, ACTIVE_RESOURCE_LOCKS, AVAILABLE_SOL_LAMPORTS,
    CAPITAL_LOCK_EXPIRED_RELEASED_TOTAL, CONCURRENT_INTENTS_GAUGE, INTENTS_EXECUTED_TOTAL,
    INTENTS_EXPIRED_TOTAL, INTENTS_RECEIVED_TOTAL, INTENTS_REJECTED_TOTAL,
    IN_FLIGHT_CAPITAL_RESERVATIONS, JITO_BUNDLES_LANDED_TOTAL, JITO_BUNDLES_REJECTED_TOTAL,
    JITO_BUNDLES_SUBMITTED_TOTAL, JITO_BUNDLES_TIMEOUT_TOTAL, JITO_TIP_LAMPORTS_TOTAL,
    KILL_SWITCH_ACTIVE, NATS_MESSAGES_RECEIVED_TOTAL, OPEN_POSITIONS_GAUGE,
    POSITION_AUTHORITY_DRIFT_LOCKMANAGER, POSITION_AUTHORITY_LOCKMANAGER_OPEN_GAUGE,
    POSITION_AUTHORITY_OPEN_GAUGE, POSITION_AUTHORITY_RECONCILE_NEEDED_GAUGE,
    PUMPSWAP_HOT_PATH_HEALING_ASYNC_PUBLISH_FAIL_TOTAL,
    PUMPSWAP_HOT_PATH_HEALING_ASYNC_PUBLISH_SUCCESS_TOTAL,
    PUMPSWAP_HOT_PATH_HEALING_COOLDOWN_SUPPRESSED_TOTAL,
    PUMPSWAP_HOT_PATH_HEALING_SKIPPED_NO_NATS_TOTAL, PUMPSWAP_HOT_PATH_HEALING_TRIGGER_TOTAL,
    REJECT_CAPITAL_LOCK, REJECT_DUPLICATE, REJECT_RESOURCE_LOCK, REJECT_SEND_FAILED,
    REJECT_SIMULATION_FAIL, REJECT_TTL_EXPIRED, SIMULATION_FAILURES_TOTAL, SIM_TIMEOUT_TOTAL,
    TX_CONFIRMED_TOTAL, TX_CONFIRM_DESERIALIZE_ERRORS_TOTAL,
    TX_CONFIRM_JETSTREAM_ORPHAN_BUFFERED_TOTAL, TX_CONFIRM_JETSTREAM_ORPHAN_EVICTED_TOTAL,
    TX_CONFIRM_JETSTREAM_ORPHAN_HIT_TOTAL, TX_CONFIRM_JETSTREAM_TOTAL, TX_CONFIRM_LATENCY_MS,
    TX_CONFIRM_TIMEOUT_TOTAL, TX_SEND_ATTEMPTS_TOTAL, TX_SEND_JITO_TOTAL, TX_SEND_RPC_TOTAL,
    TX_SEND_SUCCESS_TOTAL, TX_SEND_TPU_TOTAL, WALLET_TOTAL_SOL_LAMPORTS,
};
use ironcrab::nats::{
    config_consumer_config, config_subject, ensure_execution_results_stream,
    ensure_trade_intents_stream, pool_cache_live_consumer_config_execution_engine,
    wallet_snapshot_consumer_config, wallet_snapshot_live_consumer_config_execution_engine,
    wallet_tx_confirm_live_consumer_config_execution_engine, MomentumActivePinReason,
    MomentumActivePoolEntry, MomentumActivePoolsUpdate, NatsClient, NatsConfig, CONFIG_STREAM_NAME,
    MOMENTUM_ACTIVE_POOLS_WIRE_VERSION, STREAM_NAME, TOPIC_CONTROL_REQUESTS,
    TOPIC_CONTROL_RESPONSES, TOPIC_DECISION_RECORDS, TOPIC_EXECUTION_RESULTS, TOPIC_MARKET_EVENTS,
    TOPIC_MOMENTUM_ACTIVE_POOLS, TOPIC_PRIORITY_FEE_SAMPLES, TOPIC_TRADE_INTENTS,
    TRADE_INTENTS_STREAM_NAME, WALLET_SNAPSHOT_STREAM_NAME, WALLET_TX_CONFIRM_STREAM_NAME,
};
use ironcrab::position_authority::{
    position_authority_drift_lockmanager, reconcile_position_authority_kv_after_restart,
    PositionAuthority, PositionAuthorityChange, PositionAuthorityKvMetricsSink,
    PositionAuthorityKvPublisher,
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
    JsonlWriter, JsonlWriterConfig, SegmentRotationLimits,
};
use ironcrab::wallet::Treasury;
use parking_lot::Mutex as ParkingMutex;
use spl_token::instruction as spl_ix;
use spl_token::solana_program::program_pack::Pack;
use spl_token::solana_program::pubkey::Pubkey as SplProgPubkey;
use spl_token_2022::instruction as spl22_ix;
use spl_token_2022::{
    extension::StateWithExtensions as Spl22StateWithExtensions, state::Account as Spl22TokenAccount,
};
/// Primary `intent.resources.pools[0]`, else cache base→pool mapping.
#[inline]
fn pump_amm_pool_market_hint_merge(
    intent_pool: Option<Pubkey>,
    cache_pool: Option<Pubkey>,
) -> Option<Pubkey> {
    intent_pool.or(cache_pool)
}

/// PumpSwap AMM: pool market address for EnsurePumpAmmPoolAccounts / recovery.
/// Primary: `intent.resources.pools[0]` (explicit route + resource lock). Fallback: base→pool in LivePoolCache.
fn pump_amm_pool_market_hint_pk(intent: &TradeIntent, ctx: &ExecutionContext) -> Option<Pubkey> {
    let intent_pool = intent
        .resources
        .pools
        .first()
        .and_then(|s| Pubkey::from_str(s).ok());
    let base_mint = Pubkey::from_str(&intent.resources.input_mint).ok()?;
    let cache_pool = ctx
        .live_pool_cache
        .as_ref()
        .and_then(|c| c.get_pump_amm_pool_address_by_base_mint(&base_mint));
    if let (Some(a), Some(b)) = (intent_pool, cache_pool) {
        if a != b {
            warn!(
                intent_id = %intent.intent_id,
                intent_pool = %a,
                cache_pool = %b,
                "PumpSwap pool hint: intent.resources.pools[0] overrides LivePoolCache base→pool (differs)"
            );
        }
    }
    pump_amm_pool_market_hint_merge(intent_pool, cache_pool)
}

/// Stale/wrong PumpSwap `pool_accounts` (e.g. creator vault, protocol fee recipient) → simulation
/// Overflow / Custom(6023) / Custom(6013 InvalidProtocolFeeRecipient) / 0x1787 / 0x177d.
/// Must stay aligned with the PumpSwap cold-path recovery branch (`is_cold_path_recovery_sell` + `dex=pump_amm`).
#[inline]
fn is_pump_amm_structural_sim_error(error_code: Option<&str>) -> bool {
    error_code
        .map(|e| {
            e.contains("6023")
                || e.contains("6013")
                || e.contains("InvalidProtocolFeeRecipient")
                || e.contains("Overflow")
                || e.contains("0x1787")
                || e.contains("0x177d")
                || e.contains("0x177D")
        })
        .unwrap_or(false)
}

/// PumpFun bonding curve: stale reserves / wrong account count (cashback) → Custom(6023/6024), Overflow.
/// Cold-path structural sim matcher for PumpFun bonding-curve recovery (`is_cold_path_recovery_sell` handlers below).
#[inline]
fn is_pumpfun_bonding_curve_structural_sim_error(error_code: Option<&str>) -> bool {
    error_code
        .map(|e| {
            e.contains("6023")
                || e.contains("6024")
                || e.contains("Overflow")
                || e.contains("0x1787")
                || e.contains("0x1788")
        })
        .unwrap_or(false)
}

/// Orca Whirlpool: structural sim failure signatures (stale tick/vault layout family).
/// Cold-path recovery matcher; keep **separate** from PumpFun — same numeric strings today, different
/// programs (independent evolution).
#[inline]
fn is_orca_structural_sim_error(error_code: Option<&str>) -> bool {
    error_code
        .map(|e| {
            e.contains("6023")
                || e.contains("6024")
                || e.contains("Overflow")
                || e.contains("0x1787")
                || e.contains("0x1788")
        })
        .unwrap_or(false)
}

/// Cold Path only: DEX structural sim recovery (e.g. PumpFun bonding-curve, Orca Whirlpool) is
/// allowed for kill-switch/liquidation sells **and** explicit manual sell-all tooling
/// (`sell_all=true`).
///
/// Rationale: `sell-all` / keyless tooling may tag `sell_all=true` without `purpose=liquidation`;
/// that path is still Cold Path (not momentum hot path). PumpSwap structural sim recovery uses
/// the same gate as other cold-path DEX recovery ([`is_cold_path_recovery_sell`]).
#[inline]
fn is_cold_path_recovery_sell(intent: &TradeIntent) -> bool {
    if intent.side != TradeSide::Sell {
        return false;
    }
    if intent.metadata.get("sell_all").map(|v| v.as_str()) == Some("true") {
        return true;
    }
    intent
        .metadata
        .get("purpose")
        .map(|v| v == "liquidation")
        .unwrap_or(false)
        || intent
            .metadata
            .get("kill_switch")
            .map(|v| v == "true")
            .unwrap_or(false)
}

/// After a failed simulation, whether to run the synchronous market-data `Ensure*` recovery
/// (JetStream + bounded cache wait + one rebuild) for this DEX slice.
///
/// **Liquidation / kill-switch SELL:** any sim failure qualifies — stale SLAVE data can surface as
/// slippage, `6004`, etc., not only the structural 6013/6023/Overflow family.
///
/// **Other cold-path SELL** (e.g. `sell_all=true` without liquidation): structural signatures only,
/// so we do not pay discovery/RPC on every quote-level failure.
#[inline]
fn cold_path_dex_sim_failure_triggers_discovery_recovery(
    intent: &TradeIntent,
    error_code: Option<&str>,
    is_structural: fn(Option<&str>) -> bool,
) -> bool {
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
    if is_liquidation_sell {
        true
    } else {
        is_structural(error_code)
    }
}

/// Scope-1 async PumpSwap healing: only **regular momentum strategy** SELLs (hot path).
///
/// Excludes cold-path / manual safety tooling (e.g. `sell-all` uses `source == "sell-all"` and
/// `sell_all == "true"`), and anything already classified as liquidation/kill-switch sell via
/// `is_liquidation_sell` (`purpose=liquidation` or `kill_switch=true`).
#[inline]
fn is_regular_momentum_hot_path_sell(intent: &TradeIntent) -> bool {
    if intent.side != TradeSide::Sell {
        return false;
    }
    if intent.source != "momentum-bot" {
        return false;
    }
    // Belt-and-suspenders: sell-all / liquidation helpers may set this marker.
    if intent.metadata.get("sell_all").map(|v| v.as_str()) == Some("true") {
        return false;
    }
    // Liquidation / kill-switch sells share `source=momentum-bot` but are cold-path recovery.
    if is_cold_path_recovery_sell(intent) {
        return false;
    }
    true
}

/// Scope C: EE open-position pool pin — momentum-sourced confirmed BUY only; skip SOL/WSOL and Arb legs.
#[inline]
fn should_publish_open_position_pool_pin_after_confirmed_buy(intent: &TradeIntent) -> bool {
    if intent.side != TradeSide::Buy {
        return false;
    }
    // Dual-consumer: Arb / manual / sell-all must not publish Momentum Position pins.
    if intent.source != "momentum-bot" {
        return false;
    }
    let mint = intent.resources.output_mint.as_str();
    if mint.is_empty() || ironcrab::position_authority::is_sol_or_wsol_mint(mint) {
        return false;
    }
    intent
        .resources
        .pools
        .first()
        .is_some_and(|pool| !pool.is_empty())
}

/// I-24e (P184m): Liquidation / stale-SLAVE PumpSwap discovery must not reuse cache-first MD path.
#[inline]
fn pump_amm_liquidation_discovery_force_refresh() -> bool {
    true
}

/// P184m / Bug #36: skip hot-path simulation when SLAVE quote is not ready (avoids Custom 6004 storm).
///
/// Gates on the routed pool ([`pump_amm_pool_market_hint_pk`] / `resources.pools[0]`), not any
/// cache row for the base mint — mint-level scans can pass while the intent pool lacks reserves.
fn pump_amm_hot_path_quote_not_ready_detail(
    intent: &TradeIntent,
    cache: Option<&LivePoolCache>,
) -> Option<String> {
    if !is_regular_momentum_hot_path_sell(intent) {
        return None;
    }
    if intent.metadata.get("dex").map(|s| s.as_str()) != Some("pump_amm") {
        return None;
    }
    let base_mint = Pubkey::from_str(&intent.resources.input_mint).ok()?;
    let pool_market = intent
        .resources
        .pools
        .first()
        .and_then(|s| Pubkey::from_str(s).ok())
        .or_else(|| cache.and_then(|c| c.get_pump_amm_pool_address_by_base_mint(&base_mint)))?;
    let ready = cache
        .map(|c| match c.get(&pool_market) {
            Some(CachedPoolState::PumpAmm(ref s)) => {
                s.pool_accounts.len() >= 12
                    && matches!(
                        (s.base_reserve, s.quote_reserve),
                        (Some(b), Some(q)) if b > 0 && q > 0
                    )
            }
            _ => false,
        })
        .unwrap_or(false);
    if ready {
        return None;
    }
    Some(format!(
        "quote_not_ready: pump_amm SLAVE missing ready pool_accounts+nonzero reserves for pool {} mint {}",
        pool_market,
        intent.resources.input_mint
    ))
}

/// Scope-2: dedupe repeated async `EnsurePumpAmmPoolAccounts` publishes for the same base mint
/// (regular momentum PumpSwap SELL hot path). Static window — no new runtime config in this scope.
const PUMP_AMM_HOT_PATH_REFRESH_COOLDOWN: Duration = Duration::from_secs(30);

/// Outcome of [`try_pump_amm_hot_path_refresh_publish`]: whether to fire async NATS publish or suppress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PumpAmmHotPathRefreshDecision {
    Publish,
    Suppress { age: Duration, remaining: Duration },
}

/// Returns whether the async hot-path healing publish should run, or be suppressed by cooldown.
/// Does **not** record a cooldown start — that happens only after a successful NATS publish
/// ([`record_pump_amm_hot_path_refresh_after_success`]) so transient NATS failures do not block retries.
/// `now` is injectable for unit tests.
fn try_pump_amm_hot_path_refresh_publish(
    last_by_mint: &ParkingMutex<HashMap<Pubkey, Instant>>,
    base_mint: Pubkey,
    now: Instant,
) -> PumpAmmHotPathRefreshDecision {
    let map = last_by_mint.lock();
    match map.get(&base_mint).copied() {
        Some(prev) => {
            let age = now.saturating_duration_since(prev);
            if age < PUMP_AMM_HOT_PATH_REFRESH_COOLDOWN {
                PumpAmmHotPathRefreshDecision::Suppress {
                    age,
                    remaining: PUMP_AMM_HOT_PATH_REFRESH_COOLDOWN.saturating_sub(age),
                }
            } else {
                PumpAmmHotPathRefreshDecision::Publish
            }
        }
        None => PumpAmmHotPathRefreshDecision::Publish,
    }
}

/// Call after `nats.publish` returned `Ok(true)` for the hot-path async refresh — starts per-mint cooldown.
fn record_pump_amm_hot_path_refresh_after_success(
    last_by_mint: &ParkingMutex<HashMap<Pubkey, Instant>>,
    base_mint: Pubkey,
    now: Instant,
) {
    last_by_mint.lock().insert(base_mint, now);
}

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
    let (pre_balances, post_balances) = match (pre_balances_opt, post_balances_opt) {
        (Some(pre), Some(post)) => (pre, post),
        _ => {
            trace!(
                target_mint = %mint,
                "Token balances missing from TX meta"
            );
            return None;
        }
    };

    let owner_str = owner.to_string();

    let mut pre_sum: u128 = 0;
    let mut post_sum: u128 = 0;
    let mut decimals_opt: Option<u8> = None;
    let mut matched_pre = 0usize;
    let mut matched_post = 0usize;

    for b in pre_balances.iter() {
        let b_owner = Option::<String>::from(b.owner.clone());
        if b.mint == mint && b_owner.as_deref() == Some(&owner_str) {
            decimals_opt = Some(b.ui_token_amount.decimals);
            if let Ok(v) = u128::from_str(&b.ui_token_amount.amount) {
                pre_sum = pre_sum.saturating_add(v);
                matched_pre += 1;
            }
        }
    }

    for b in post_balances.iter() {
        let b_owner = Option::<String>::from(b.owner.clone());
        if b.mint == mint && b_owner.as_deref() == Some(&owner_str) {
            decimals_opt = Some(b.ui_token_amount.decimals);
            if let Ok(v) = u128::from_str(&b.ui_token_amount.amount) {
                post_sum = post_sum.saturating_add(v);
                matched_post += 1;
            }
        }
    }

    // Debug logging when no balance found for the target mint
    if decimals_opt.is_none() {
        // Log available mints for debugging owner mismatch issues
        let available_mints: Vec<&str> = post_balances.iter().map(|b| b.mint.as_str()).collect();
        trace!(
            target_mint = %mint,
            target_owner = %owner_str,
            available_mints = ?available_mints,
            pre_balance_count = pre_balances.len(),
            post_balance_count = post_balances.len(),
            "No token balance found for mint/owner combination"
        );
        return None;
    }

    let decimals = decimals_opt?;
    let delta = post_sum as i128 - pre_sum as i128;

    trace!(
        target_mint = %mint,
        pre_sum = pre_sum,
        post_sum = post_sum,
        delta = delta,
        matched_pre = matched_pre,
        matched_post = matched_post,
        "Token balance delta computed"
    );

    Some((decimals, delta))
}

/// Hard evidence from confirmed transaction meta: the wallet no longer has any token balance
/// for `mint` after the tx (typical SPL `close_account` on the wallet ATA).
///
/// **Not** inferred from intent metadata (`close_token_ata`) — partial sells can still carry
/// that flag while the ATA stays open (Scope 48 production case).
fn tx_meta_wallet_token_balance_absent_after_tx(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    wallet: &Pubkey,
    mint: &str,
) -> bool {
    let Some(meta) = tx.transaction.meta.as_ref() else {
        return false;
    };
    let Some(pre_balances) =
        Option::<Vec<UiTransactionTokenBalance>>::from(meta.pre_token_balances.clone())
    else {
        return false;
    };
    let Some(post_balances) =
        Option::<Vec<UiTransactionTokenBalance>>::from(meta.post_token_balances.clone())
    else {
        return false;
    };

    let owner_str = wallet.to_string();
    let mut pre_sum: u128 = 0;
    for b in pre_balances.iter() {
        let b_owner = Option::<String>::from(b.owner.clone());
        if b.mint == mint && b_owner.as_deref() == Some(owner_str.as_str()) {
            if let Ok(v) = u128::from_str(&b.ui_token_amount.amount) {
                pre_sum = pre_sum.saturating_add(v);
            }
        }
    }
    if pre_sum == 0 {
        return false;
    }

    let mut post_sum: u128 = 0;
    for b in post_balances.iter() {
        let b_owner = Option::<String>::from(b.owner.clone());
        if b.mint == mint && b_owner.as_deref() == Some(owner_str.as_str()) {
            if let Ok(v) = u128::from_str(&b.ui_token_amount.amount) {
                post_sum = post_sum.saturating_add(v);
            }
        }
    }
    post_sum == 0
}

/// Scope 48: after a confirmed SELL, whether we treat the position as fully closed for
/// LockManager / `ExecutionResult` metadata.
///
/// - **Cold-path recovery** ([`is_cold_path_recovery_sell`]: liquidation, kill-switch, sell-all):
///   always full-close (conservative).
/// - **Regular Momentum / other SELLs**: full-close only when sold amount covers the engine's
///   pre-release position (`sold_raw >= total_pos`) **or** transaction meta proves the wallet
///   no longer holds any balance for the mint (actual ATA close / zero balance).
///
/// `sell_token_account_closed` metadata is emitted **only** with hard tx-meta evidence, never
/// from `close_token_ata` intent metadata alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Scope48ConfirmedSellCloseDecision {
    full_close: bool,
    sell_token_account_closed: bool,
    sell_untracked_ata: bool,
}

fn scope48_confirmed_sell_close_decision(
    is_cold_path_recovery: bool,
    sold_raw: u64,
    total_pos: u64,
    wallet_token_balance_absent_after_tx: bool,
) -> Scope48ConfirmedSellCloseDecision {
    let regular_full_close = sold_raw >= total_pos || wallet_token_balance_absent_after_tx;
    let full_close = is_cold_path_recovery || regular_full_close;
    let sell_token_account_closed = wallet_token_balance_absent_after_tx;
    let sell_untracked_ata = full_close && !sell_token_account_closed;
    Scope48ConfirmedSellCloseDecision {
        full_close,
        sell_token_account_closed,
        sell_untracked_ata,
    }
}

/// Scope 48: always set `sell_position_delta_applied` / optional flags on confirmed SELL
/// `ExecutionResult`s from this engine, using lock snapshot + intent when tx fills are missing.
fn apply_scope48_confirmed_sell_execution_metadata(
    exec: &mut ExecutionResult,
    lock_manager: &LockManager,
    intent: &TradeIntent,
    confirmed_sell_fill_in_raw: Option<u64>,
    wallet_token_balance_absent_after_tx: bool,
) {
    if intent.side != TradeSide::Sell || exec.status != ExecutionStatus::Confirmed {
        return;
    }
    let mint_key = intent.resources.input_mint.clone();
    let (avail, locked) =
        lock_manager.available_and_locked_tokens_for_intent(&intent.intent_id, &mint_key);
    let total_pos = lock_manager
        .intent_token_position_total_at_lock(&intent.intent_id, &mint_key)
        .unwrap_or_else(|| avail.saturating_add(locked));
    let sold_raw = confirmed_sell_fill_in_raw.unwrap_or(intent.required_capital.raw);
    let is_cold_path_recovery = is_cold_path_recovery_sell(intent);
    let s48 = scope48_confirmed_sell_close_decision(
        is_cold_path_recovery,
        sold_raw,
        total_pos,
        wallet_token_balance_absent_after_tx,
    );

    if s48.sell_token_account_closed {
        exec.metadata
            .insert("sell_token_account_closed".to_string(), "true".to_string());
    } else {
        exec.metadata.remove("sell_token_account_closed");
    }
    exec.metadata.insert(
        "sell_position_delta_applied".to_string(),
        if s48.full_close {
            "full".to_string()
        } else {
            "partial".to_string()
        },
    );
    if s48.sell_untracked_ata {
        exec.metadata
            .insert("sell_untracked_ata".to_string(), "true".to_string());
    } else {
        exec.metadata.remove("sell_untracked_ata");
    }
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

/// Returns (delta, fee, has_lifecycle_noise, rent_adjustment).
///
/// `rent_adjustment` is the sum of lamports the wallet spent on creating new accounts
/// minus the lamports received from closing accounts.  Subtracting this from the raw
/// delta isolates the swap + fee portion of the lamport change.
///
/// Positive rent_adjustment = wallet paid rent (accounts created).
/// Negative rent_adjustment = wallet received rent back (accounts closed).
fn compute_wallet_lamport_delta_best_effort(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    wallet: &Pubkey,
) -> Option<(i128, u64, bool, i128)> {
    let meta = tx.transaction.meta.as_ref()?;
    let wallet_index = find_message_account_index(tx, wallet)?;

    let pre = *meta.pre_balances.get(wallet_index)?;
    let post = *meta.post_balances.get(wallet_index)?;
    let delta = post as i128 - pre as i128;

    // Heuristic: if the tx funds a brand new account or zeroes an account in the message,
    // payer lamport delta is likely polluted by rent / account lifecycle noise.
    let mut has_account_lifecycle_noise = false;
    // Track rent paid (accounts created) and rent refunded (accounts closed).
    let mut rent_adjustment: i128 = 0;
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
            // New account created — wallet likely paid rent = post_i lamports
            has_account_lifecycle_noise = true;
            rent_adjustment += post_i as i128;
        }
        if pre_i > 0 && post_i == 0 {
            // Account closed — wallet likely received rent refund = pre_i lamports
            has_account_lifecycle_noise = true;
            rent_adjustment -= pre_i as i128;
        }
    }

    Some((
        delta,
        meta.fee,
        has_account_lifecycle_noise,
        rent_adjustment,
    ))
}

/// Extract SOL swap amounts from parsed inner instructions.
///
/// Parses `meta.inner_instructions` for System program `transfer` instructions
/// involving the wallet.  Returns `(sol_out, sol_in)` where:
/// - `sol_out`: total lamports transferred FROM wallet via `transfer` (not `createAccount`)
/// - `sol_in`:  total lamports transferred TO wallet via `transfer`
///
/// `createAccount` instructions are excluded because they represent ATA rent, not swap payments.
/// This gives a much more accurate swap amount than the raw lamport delta.
fn extract_swap_sol_from_inner_instructions(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    wallet: &Pubkey,
) -> (u64, u64) {
    let wallet_str = wallet.to_string();
    let mut sol_out: u64 = 0; // lamports FROM wallet (e.g., swap payment on BUY)
    let mut sol_in: u64 = 0; // lamports TO wallet (e.g., swap proceeds on SELL)

    let meta = match tx.transaction.meta.as_ref() {
        Some(m) => m,
        None => return (0, 0),
    };

    // Serialize inner_instructions to JSON for easy traversal.
    // The Solana SDK uses nested enums (UiInstruction → UiParsedInstruction → ParsedInstruction)
    // which are cumbersome to match through.  JSON traversal is simpler and resilient to
    // SDK version differences.
    let inner_json = match serde_json::to_value(&meta.inner_instructions) {
        Ok(v) => v,
        Err(_) => return (0, 0),
    };

    let groups = match inner_json.as_array() {
        Some(g) => g,
        None => return (0, 0),
    };

    for group in groups {
        let instructions = match group.get("instructions").and_then(|v| v.as_array()) {
            Some(ixs) => ixs,
            None => continue,
        };

        for ix in instructions {
            // We want fully-parsed System program instructions.
            // In JsonParsed encoding these appear as:
            //   { "parsed": { "type": "transfer", "info": { "source", "destination", "lamports" } },
            //     "program": "system", "programId": "1111..." }
            // OR nested inside a "Parsed"/"Compiled" wrapper depending on SDK serialization.

            // Try direct access first (common for inner instructions)
            let parsed_obj = ix
                .get("parsed")
                // Also handle the case where the SDK wraps in { "Parsed": { "parsed": ... } }
                .or_else(|| ix.get("Parsed").and_then(|p| p.get("parsed")));

            let program = ix.get("program").and_then(|p| p.as_str()).or_else(|| {
                ix.get("Parsed")
                    .and_then(|p| p.get("program"))
                    .and_then(|p| p.as_str())
            });

            let program_id = ix.get("programId").and_then(|p| p.as_str()).or_else(|| {
                ix.get("Parsed")
                    .and_then(|p| p.get("programId"))
                    .and_then(|p| p.as_str())
            });

            // Must be System program
            let is_system =
                program == Some("system") || program_id == Some("11111111111111111111111111111111");
            if !is_system {
                continue;
            }

            let parsed = match parsed_obj {
                Some(p) => p,
                None => continue,
            };

            let ix_type = parsed.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let info = match parsed.get("info") {
                Some(i) => i,
                None => continue,
            };

            // Skip createAccount — that's ATA rent, not swap
            if ix_type == "createAccount" || ix_type == "createAccountWithSeed" {
                continue;
            }

            if ix_type == "transfer" {
                let source = info.get("source").and_then(|s| s.as_str()).unwrap_or("");
                let destination = info
                    .get("destination")
                    .and_then(|d| d.as_str())
                    .unwrap_or("");
                let lamports = info.get("lamports").and_then(|l| l.as_u64()).unwrap_or(0);

                if lamports == 0 {
                    continue;
                }

                if source == wallet_str && destination != wallet_str {
                    sol_out = sol_out.saturating_add(lamports);
                } else if destination == wallet_str && source != wallet_str {
                    sol_in = sol_in.saturating_add(lamports);
                }
            }
        }
    }

    (sol_out, sol_in)
}

/// Best-effort fill accounting from confirmed transaction RPC fetch (cold path after send).
#[derive(Debug, Clone)]
struct ComputedIntentFills {
    fill_in: Option<ExplicitAmount>,
    fill_out: Option<ExplicitAmount>,
    fill_status: FillStatus,
    fill_unavailable_reason: Option<FillUnavailableReason>,
    wallet_sol_delta: Option<i128>,
    /// On-chain block time from RPC tx meta (seconds → ms), cold path only.
    block_time_unix_ms: Option<u64>,
    /// True only from tx meta: wallet had this mint pre-tx and no post-tx token balance for it.
    wallet_token_balance_absent_after_tx: bool,
}

async fn compute_intent_fills_best_effort(
    ctx: &ExecutionContext,
    wallet: Pubkey,
    signature: &Signature,
    intent: &TradeIntent,
) -> ComputedIntentFills {
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
            return ComputedIntentFills {
                fill_in: None,
                fill_out: None,
                fill_status: FillStatus::Unavailable,
                fill_unavailable_reason: Some(FillUnavailableReason::RpcTxFetchFailed),
                wallet_sol_delta: None,
                block_time_unix_ms: None,
                wallet_token_balance_absent_after_tx: false,
            };
        }
    };

    let block_time_unix_ms = tx
        .block_time
        .and_then(|bt| (bt > 0).then(|| (bt as u64).saturating_mul(1000)));

    if tx.transaction.meta.is_none() {
        return ComputedIntentFills {
            fill_in: None,
            fill_out: None,
            fill_status: FillStatus::Unavailable,
            fill_unavailable_reason: Some(FillUnavailableReason::TxMetaMissing),
            wallet_sol_delta: None,
            block_time_unix_ms,
            wallet_token_balance_absent_after_tx: false,
        };
    }

    let input_mint = intent.resources.input_mint.as_str();
    let output_mint = intent.resources.output_mint.as_str();

    // ARBITRAGE DETECTION: input_mint == output_mint == WSOL means it's an arb cycle
    let is_arb_cycle = input_mint == output_mint && input_mint == SOL_MINT;

    // Get native SOL (lamport) delta for wallet - used for fees and native SOL tracking
    let mut lamport_reason: Option<FillUnavailableReason> = None;
    let (payer_delta_lamports, fee_lamports, lamport_noise, rent_adjustment) =
        match compute_wallet_lamport_delta_best_effort(&tx, &wallet) {
            Some((d, f, noise, rent)) => {
                if noise {
                    lamport_reason =
                        Some(FillUnavailableReason::LamportDeltaGatedAccountLifecycleNoise);
                }
                (d, f, noise, rent)
            }
            None => {
                lamport_reason = Some(FillUnavailableReason::WalletAccountIndexMissing);
                (0, 0, true, 0)
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

            return ComputedIntentFills {
                fill_in,
                fill_out,
                fill_status: FillStatus::Complete,
                fill_unavailable_reason: None,
                wallet_sol_delta: Some(total_sol_delta),
                block_time_unix_ms,
                wallet_token_balance_absent_after_tx: false,
            };
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

            return ComputedIntentFills {
                fill_in,
                fill_out,
                fill_status: FillStatus::Partial,
                fill_unavailable_reason: Some(FillUnavailableReason::TokenBalanceDeltaMissing),
                wallet_sol_delta: Some(payer_delta_lamports),
                block_time_unix_ms,
                wallet_token_balance_absent_after_tx: false,
            };
        }
    }

    // ============ REGULAR BUY/SELL HANDLING ============
    // Primary path: use token-balance deltas (works for most SPL tokens, including WSOL if used).
    let input_token_delta = extract_owner_mint_delta_raw(&tx, &wallet, input_mint);
    let output_token_delta = extract_owner_mint_delta_raw(&tx, &wallet, output_mint);

    // Pre-compute inner-instruction SOL transfers for the lamport_noise fallback paths.
    // Only needed when one leg is native SOL and token-balance deltas are unavailable.
    let (ix_sol_out, ix_sol_in) = if (input_mint == SOL_MINT || output_mint == SOL_MINT)
        && (input_token_delta.is_none() || output_token_delta.is_none())
    {
        extract_swap_sol_from_inner_instructions(&tx, &wallet)
    } else {
        (0, 0)
    };

    let fill_in = if let Some((decimals, delta)) = input_token_delta {
        if delta < 0 {
            Some(ExplicitAmount::new((-delta) as u64, decimals))
        } else {
            None
        }
    } else if input_mint == SOL_MINT && !lamport_noise && payer_delta_lamports < 0 {
        // No lifecycle noise: use payer delta minus fee as swap approximation.
        let spent_total = (-payer_delta_lamports) as u64;
        let spent_ex_fee = spent_total.saturating_sub(fee_lamports);
        if spent_ex_fee > 0 {
            Some(ExplicitAmount::new(spent_ex_fee, 9))
        } else {
            None
        }
    } else if input_mint == SOL_MINT && lamport_noise {
        // BUY with account lifecycle noise (new ATA created).
        //
        // Priority 1: Inner-instruction parsing — sum of System.transfer OUT from wallet.
        //   This includes swap payment + DEX fees + Jito tips but excludes ATA rent
        //   (createAccount instructions are filtered out).
        //   For normal trades, the swap is the dominant transfer.
        //
        // Priority 2: Rent-adjusted lamport delta — removes ATA rent from raw delta.
        //   Still includes priority fees and program-level fees but vastly more accurate
        //   than intent.required_capital.
        //
        // Priority 3 (last resort): intent.required_capital — can be 29x wrong when
        //   the DEX only accepts a fraction of the intended SOL (e.g., bonding curve
        //   nearly complete).
        if ix_sol_out > 0 {
            debug!(
                ix_sol_out = ix_sol_out,
                intent_capital = intent.required_capital.raw,
                "fill_in from inner instructions (System.transfer OUT, excl. createAccount)"
            );
            Some(ExplicitAmount::new(ix_sol_out, 9))
        } else {
            // Rent-adjusted fallback: |delta| - rent_paid - fee ≈ swap + program-level fees
            // rent_adjustment is positive when wallet paid rent (ATA created), so subtract it.
            let adjusted =
                ((-payer_delta_lamports) - rent_adjustment).saturating_sub(fee_lamports as i128);
            if adjusted > 0 {
                debug!(
                    adjusted = adjusted,
                    raw_delta = payer_delta_lamports,
                    rent_adjustment = rent_adjustment,
                    fee = fee_lamports,
                    intent_capital = intent.required_capital.raw,
                    "fill_in from rent-adjusted lamport delta"
                );
                Some(ExplicitAmount::new(adjusted as u64, 9))
            } else {
                // Last resort — can be wildly wrong (documented in BUGS_FIXES.md)
                warn!(
                    intent_capital = intent.required_capital.raw,
                    raw_delta = payer_delta_lamports,
                    rent_adjustment = rent_adjustment,
                    "fill_in falling back to intent.required_capital — may be inaccurate!"
                );
                Some(intent.required_capital.clone())
            }
        }
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
        // No lifecycle noise: use payer delta plus fee as swap approximation.
        let received_total = payer_delta_lamports as u64;
        let received_plus_fee = received_total.saturating_add(fee_lamports);
        if received_plus_fee > 0 {
            Some(ExplicitAmount::new(received_plus_fee, 9))
        } else {
            None
        }
    } else if output_mint == SOL_MINT && lamport_noise {
        // SELL with account lifecycle noise (ATA closed, rent refunded).
        // Same priority chain as BUY fill_in but for the incoming SOL side.
        if ix_sol_in > 0 {
            debug!(
                ix_sol_in = ix_sol_in,
                "fill_out from inner instructions (System.transfer IN)"
            );
            Some(ExplicitAmount::new(ix_sol_in, 9))
        } else {
            // Rent-adjusted: raw_delta - rent_refund + fee ≈ swap proceeds
            let adjusted =
                (payer_delta_lamports + rent_adjustment).saturating_add(fee_lamports as i128);
            if adjusted > 0 {
                debug!(
                    adjusted = adjusted,
                    raw_delta = payer_delta_lamports,
                    rent_adjustment = rent_adjustment,
                    fee = fee_lamports,
                    "fill_out from rent-adjusted lamport delta"
                );
                Some(ExplicitAmount::new(adjusted as u64, 9))
            } else {
                None
            }
        }
    } else {
        None
    };

    let fill_status = match (fill_in.is_some(), fill_out.is_some()) {
        (true, true) => FillStatus::Complete,
        (true, false) | (false, true) => FillStatus::Partial,
        (false, false) => FillStatus::Unavailable,
    };

    let wallet_token_balance_absent_after_tx =
        if intent.side == TradeSide::Sell && intent.resources.input_mint != SOL_MINT {
            tx_meta_wallet_token_balance_absent_after_tx(
                &tx,
                &wallet,
                intent.resources.input_mint.as_str(),
            )
        } else {
            false
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

    // Debug logging for incomplete fills (helps diagnose dashboard issues)
    if fill_status != FillStatus::Complete {
        debug!(
            input_mint = %input_mint,
            output_mint = %output_mint,
            fill_in_present = fill_in.is_some(),
            fill_out_present = fill_out.is_some(),
            fill_status = ?fill_status,
            fill_reason = ?fill_unavailable_reason,
            lamport_noise = lamport_noise,
            payer_delta_lamports = payer_delta_lamports,
            "Fill accounting incomplete - dashboard values may be inaccurate"
        );
    }

    // Return wallet SOL delta (5th tuple element) unless gated by noise
    // Always return wallet SOL delta for accurate PnL tracking.
    // Previously gated by !lamport_noise, but the delta IS the correct total cost
    // (including ATA rent, fees, etc.) which is exactly what PnL needs.
    let wallet_sol_delta = Some(payer_delta_lamports);

    ComputedIntentFills {
        fill_in,
        fill_out,
        fill_status,
        fill_unavailable_reason,
        wallet_sol_delta,
        block_time_unix_ms,
        wallet_token_balance_absent_after_tx,
    }
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

    /// Golden Replay Mode: Liest Intents aus JSONL, schreibt Decisions in JSONL, kein NATS/RPC.
    #[arg(long)]
    replay: bool,

    /// Pfad zur Intents-JSONL (nur bei --replay)
    #[arg(long, requires = "replay")]
    replay_intents: Option<PathBuf>,

    /// Pfad zur Output-Decisions-JSONL (nur bei --replay)
    #[arg(long, requires = "replay")]
    replay_output: Option<PathBuf>,
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

    /// Default intent TTL (ms) when intent.ttl_ms is missing or zero
    intent_ttl_ms: u64,

    /// Confirmation timeout (ms) for RPC send path
    confirmation_timeout_ms: u64,

    /// Buffer (ms) added to discovery + simulation when computing pre-send capital-lock TTL.
    capital_lock_ttl_buffer_ms: u64,

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

    // === FIX-31: Parallel Intent Processing ===
    /// Maximum number of intents processed concurrently. Startup-only (restart required).
    max_concurrent_intents: u32,

    // === PR3: JetStream TX Confirmation (market-data Geyser → JetStream) ===
    /// Wait for `WalletTxConfirmed` on JetStream (no EE Geyser, no RPC fallback).
    jetstream_tx_confirm_enabled: bool,
    /// Commitment for TX confirmation: `"confirmed"` (default, lower latency, reorg risk) or `"finalized"` (slower, stricter finality).
    confirm_commitment: String,
    /// Rebroadcast interval during confirmation wait (ms)
    rebroadcast_interval_ms: u64,
    /// Max rebroadcasts per TX during confirmation
    max_rebroadcasts: u32,
    /// Rebroadcast via TPU (TxSender) when available; RPC fallback on failure.
    rebroadcast_use_tpu: bool,

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

    /// PA-6b: EE publishes PositionAuthority KV when true (rollback only; default false).
    publish_position_authority_kv: bool,
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
            simulation_timeout_ms: 500,
            intent_ttl_ms: 5_000,
            confirmation_timeout_ms: 15_000,
            capital_lock_ttl_buffer_ms: 10_000,
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
            // FIX-31: Parallel intent processing
            max_concurrent_intents: 4,
            // PR3: JetStream TX confirmation (product default: enabled — see CONFIG_SCHEMA)
            jetstream_tx_confirm_enabled: true,
            confirm_commitment: "confirmed".to_string(),
            rebroadcast_interval_ms: 2_000,
            max_rebroadcasts: 5,
            rebroadcast_use_tpu: true,
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
            publish_position_authority_kv: false,
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
            open_positions: ctx.get_open_positions(),
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
    /// When `Some`, this is already the engine `min_out` in lamports (LivePoolCache quote path)
    /// and must not be run through slippage scaling again in multi-pool fallback.
    execution_min_out_lamports: Option<u64>,
}

/// JSON array of additional buildable routes (same shape as [`RouteCandidate`]) for cold-path
/// multi-pool SELL fallback after `build_tx_plan` Unsupported (liquidation / kill-switch).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MultiPoolFallbackRouteWire {
    dex: String,
    pool_id: String,
    amount_out: u64,
    accounts: Vec<String>,
    #[serde(default)]
    creator: Option<String>,
    #[serde(default)]
    execution_min_out_lamports: Option<u64>,
}

const MULTI_POOL_FALLBACK_ROUTES_JSON_META_KEY: &str = "multi_pool_fallback_routes_json";

fn sort_route_candidates_by_amount_out(mut v: Vec<RouteCandidate>) -> Vec<RouteCandidate> {
    v.sort_by(|a, b| b.amount_out.cmp(&a.amount_out));
    v
}

/// Cold-path liquidation: drop routes that would fail the same structural gates as
/// `tx_builder::build_tx_plan` (Orca tick-array RPC check, PumpSwap SLAVE ≥12 accounts, Meteora
/// explicit Ready). Preserves descending `amount_out` order among survivors.
async fn liquidation_filter_multi_pool_buildable_candidates(
    live_cache: Option<&LivePoolCache>,
    orca: &Orca,
    mint: &Pubkey,
    sol_mint: &Pubkey,
    candidates: Vec<RouteCandidate>,
    quote_attempts: &mut Vec<String>,
) -> Vec<RouteCandidate> {
    let ordered = sort_route_candidates_by_amount_out(candidates);
    let mut out = Vec::new();
    for c in ordered {
        match c.dex.as_str() {
            "orca" => {
                let Ok(pool_pk) = Pubkey::from_str(&c.pool_id) else {
                    quote_attempts.push(format!("orca=skip bad_pool_pubkey pool={}", c.pool_id));
                    continue;
                };
                match orca
                    .cold_path_validate_whirlpool_tick_arrays_for_pool_swap(
                        &pool_pk, mint, sol_mint,
                    )
                    .await
                {
                    Ok(()) => out.push(c),
                    Err(reason) => {
                        info!(
                            dex = %c.dex,
                            pool = %c.pool_id,
                            reason = %reason,
                            "routing skipped orca: not buildable; selected next if any"
                        );
                        quote_attempts.push(format!(
                            "orca=skip not_buildable pool={} reason={reason}",
                            c.pool_id
                        ));
                    }
                }
            }
            "pump_amm" => {
                let Ok(pool_pk) = Pubkey::from_str(&c.pool_id) else {
                    quote_attempts
                        .push(format!("pump_amm=skip bad_pool_pubkey pool={}", c.pool_id));
                    continue;
                };
                if !c.accounts.is_empty() {
                    if let Ok(first) = Pubkey::from_str(&c.accounts[0]) {
                        if first != pool_pk {
                            info!(
                                pool_hint = %c.pool_id,
                                accounts0 = %first,
                                "routing skipped pump_amm: not buildable (reason=pool_account_mismatch); selected next if any"
                            );
                            quote_attempts.push(format!(
                                "pump_amm=skip not_buildable pool_mismatch pool={}",
                                c.pool_id
                            ));
                            continue;
                        }
                    }
                }
                let cache_ok = live_cache.is_some_and(|cache| {
                    pump_amm_hint_pool_cache_usable_for_tx_plan_builder(cache, &pool_pk)
                });
                if !cache_ok {
                    info!(
                        pool = %c.pool_id,
                        "routing skipped pump_amm: not buildable (reason=slave_pool_accounts_not_ready); selected next if any"
                    );
                    quote_attempts.push(format!(
                        "pump_amm=skip not_buildable no_builder_ready_cache pool={}",
                        c.pool_id
                    ));
                    continue;
                }
                out.push(c);
            }
            "meteora_dlmm" => {
                let Ok(pool_pk) = Pubkey::from_str(&c.pool_id) else {
                    quote_attempts.push(format!("meteora=skip bad_pool_pubkey pool={}", c.pool_id));
                    continue;
                };
                let ready = live_cache
                    .map(|cache| cache.meteora_dlmm_pool_explicitly_ready(&pool_pk))
                    .unwrap_or(false);
                if !ready {
                    info!(
                        pool = %c.pool_id,
                        "routing skipped meteora_dlmm: not buildable (reason=no_explicit_ready); selected next if any"
                    );
                    quote_attempts.push(format!(
                        "meteora=skip not_buildable no_explicit_ready pool={}",
                        c.pool_id
                    ));
                    continue;
                }
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

fn liquidation_store_multi_pool_fallback_metadata(
    metadata: &mut HashMap<String, String>,
    buildable: &[RouteCandidate],
) {
    if buildable.len() <= 1 {
        metadata.remove(MULTI_POOL_FALLBACK_ROUTES_JSON_META_KEY);
        return;
    }
    let tail: Vec<MultiPoolFallbackRouteWire> = buildable
        .iter()
        .skip(1)
        .map(|c| MultiPoolFallbackRouteWire {
            dex: c.dex.clone(),
            pool_id: c.pool_id.clone(),
            amount_out: c.amount_out,
            accounts: c.accounts.clone(),
            creator: c.creator.clone(),
            execution_min_out_lamports: c.execution_min_out_lamports,
        })
        .collect();
    if let Ok(json) = serde_json::to_string(&tail) {
        metadata.insert(MULTI_POOL_FALLBACK_ROUTES_JSON_META_KEY.to_string(), json);
    }
}

/// After `build_tx_plan` returns Unsupported for a cold-path multi-pool sell: apply the next
/// pre-filtered buildable route (if any). Returns true when `intent` was updated for a retry.
fn take_next_multi_pool_buildable_fallback_route(intent: &mut TradeIntent) -> bool {
    if intent.metadata.get("sell_routing").map(|s| s.as_str()) != Some("multi_pool") {
        return false;
    }
    let json = match intent
        .metadata
        .get(MULTI_POOL_FALLBACK_ROUTES_JSON_META_KEY)
    {
        Some(j) if !j.is_empty() => j.clone(),
        _ => return false,
    };
    let mut routes: Vec<MultiPoolFallbackRouteWire> = match serde_json::from_str(&json) {
        Ok(r) => r,
        Err(_) => return false,
    };
    if routes.is_empty() {
        return false;
    }
    let next = routes.remove(0);
    let rest = serde_json::to_string(&routes).unwrap_or_else(|_| "[]".to_string());
    intent
        .metadata
        .insert(MULTI_POOL_FALLBACK_ROUTES_JSON_META_KEY.to_string(), rest);

    let min_lamports = next.execution_min_out_lamports.unwrap_or_else(|| {
        let keep_bps = 10_000u64.saturating_sub(intent.max_slippage_bps as u64);
        ((next.amount_out as u128) * (keep_bps as u128) / 10_000u128) as u64
    });
    intent.metadata.insert("dex".to_string(), next.dex.clone());
    intent.resources.pools = vec![next.pool_id.clone()];
    intent.resources.accounts = next.accounts.clone();
    match &next.creator {
        Some(c) => {
            intent.metadata.insert("creator".to_string(), c.clone());
        }
        None => {
            intent.metadata.remove("creator");
        }
    }
    intent.execution = Some(TradeExecutionConstraints {
        min_out: Some(ExplicitAmount::new(min_lamports, 9)),
    });
    true
}

/// Guard timeout for `PumpFunAmmDex::quote_exact_in` in liquidation / 6005-retry.
/// Must match the `tokio::time::timeout` duration at call sites; Scope 51: do not reduce.
const PUMPSWAP_LIQUIDATION_QUOTE_TIMEOUT_SECS: u64 = 45;

#[inline]
fn pump_amm_liquidation_quote_timeout_str() -> String {
    format!(
        "pump_amm=timeout ({}s)",
        PUMPSWAP_LIQUIDATION_QUOTE_TIMEOUT_SECS
    )
}

/// Scope 51: Cold-path **PumpFun bonding-curve SELL first** when cache proves an **active** curve.
///
/// - `true`: prefer direct PumpFun SELL before multi-pool (PumpSwap) discovery.
/// - `false` when a curve row is known **complete/migrated** (`pumpfun_bonding_curve_complete_for_mint`),
///   or when Geyser state is **unknown** (cache miss on `is_pumpfun_complete_for_mint`) — safe
///   default keeps multi-pool first to avoid 6005 from theoretical PumpFun quotes on completed curves.
#[inline]
fn liquidation_pumpfun_sell_preference(
    live_cache: Option<&LivePoolCache>,
    base_mint: &Pubkey,
) -> bool {
    if let Some(c) = live_cache {
        if c.pumpfun_bonding_curve_complete_for_mint(base_mint) {
            return false;
        }
        if c.is_pumpfun_complete_for_mint(base_mint) == Some(false) {
            return true;
        }
    }
    false
}

/// Cached blockhash from Geyser blocks_meta stream (via market-data NATS).
/// Used to avoid RPC `getLatestBlockhash` calls in the hot path.
#[derive(Debug, Clone)]
struct CachedBlockhash {
    hash: solana_sdk::hash::Hash,
    slot: u64,
    #[allow(dead_code)] // Kept for future freshness checks (e.g. last_valid_block_height)
    block_height: u64,
    received_at: std::time::Instant,
}

/// Blockhash + slot used to sign a TX (must stay paired for `slot_at_send` metrics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockhashForSend {
    hash: solana_sdk::hash::Hash,
    slot: u64,
}

/// Persist RPC-fallback blockhash so later readers see the same slot as the signing hash.
fn apply_rpc_blockhash_cache_fallback(
    cache: &mut Option<CachedBlockhash>,
    hash: solana_sdk::hash::Hash,
    slot: u64,
    received_at: std::time::Instant,
) {
    *cache = Some(CachedBlockhash {
        hash,
        slot,
        block_height: 0,
        received_at,
    });
}

/// Maximum age (in seconds) before we fall back to RPC for blockhash.
/// Solana blockhashes expire after ~150 slots (~60s). We use a conservative 30s.
const MAX_BLOCKHASH_AGE_SECS: u64 = 30;

/// I-24d: Bounded wait timeout for Discovery Request/Reply (market-data).
/// Scope-40: local-validator `getProgramAccounts` fallback for PumpSwap can be ~25s; 45s budget.
const DISCOVERY_REQUEST_TIMEOUT_SECS: u64 = 45;

/// I-24d: Bounded wait for authoritative SLAVE pool state after a successful discovery/recovery reply
/// (JetStream `PoolCacheUpdate` delivery + merge into `LivePoolCache`). Call sites run only after
/// `DiscoveryRequestOutcome::Ok`, so this must **not** re-budget [`DISCOVERY_REQUEST_TIMEOUT_SECS`].
const DISCOVERY_CACHE_WAIT_TIMEOUT_MS: u64 = 10_000;
/// PumpSwap liquidation cold-path: extra JetStream wait after force_refresh (P184e).
const PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS: u64 = 20_000;

/// I-24d: Poll interval when waiting for usable PumpAmm cache state in SLAVE cache.
const DISCOVERY_CACHE_POLL_INTERVAL_MS: u64 = 100;

/// Outcome of a Discovery Request (I-24d). Used for bounded wait + single retry.
#[derive(Debug)]
enum DiscoveryRequestOutcome {
    Ok,
    NotFound,
    Error(String),
    Timeout,
}

/// I-24d: Bounded wait until PumpSwap AMM is usable for quotes **and** swap account lists in SLAVE cache (JetStream path).
///
/// `ControlResponse=Ok` can correlate before `apply_pool_cache_update` exposes non-degenerate
/// `base_reserve`/`quote_reserve` (Bug #27): `pool_accounts` alone is a weak readiness signal.
/// Bug #36: non-empty `pool_accounts` must not be treated as hot-path ready without explicit
/// [`DexPoolReadiness::Ready`] (or legacy authoritative heuristic inside
/// [`LivePoolCache::get_ready_pump_amm_pool_accounts_by_base_mint`]).
/// Polls until both `pump_amm_quote_ready_by_base_mint` and
/// `pump_amm_swap_accounts_ready_by_base_mint` are true or timeout.
async fn wait_for_usable_pump_amm_cache_state(
    cache: &LivePoolCache,
    base_mint: &Pubkey,
    timeout_ms: u64,
    poll_interval_ms: u64,
) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    while std::time::Instant::now() < deadline {
        if cache.pump_amm_quote_ready_by_base_mint(base_mint)
            && cache.pump_amm_swap_accounts_ready_by_base_mint(base_mint)
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }
    false
}

/// Evidence tuple for PumpSwap cold-path force-refresh wait (Bug #34 / #36, P184h).
type PumpAmmSlaveRecoveryEvidence = (
    u64,            // cache entry slot
    Option<u64>,    // base_reserve
    Option<u64>,    // quote_reserve
    usize,          // pool_accounts.len()
    bool,           // sell_extended
    Option<Pubkey>, // sell_third_meta (ix #23)
    Option<Pubkey>, // sell_extended_tail_0 (ix #21)
    Option<Pubkey>, // sell_extended_tail_1 (ix #22)
    bool,           // sell_layout_ready
    bool,           // sell_requires_pre_fee_metas (27-account SSOT)
    u8,             // inferred sell_ix_account_count
    Option<Pubkey>, // sell_pre_fee_meta_1 (ix #20 on 27-account layout)
    u64,            // layout_generation (monotonic per pool)
);

/// Snapshot of the PumpSwap SLAVE row for the specific `pool`.
#[inline]
fn pump_amm_slave_recovery_snapshot(
    cache: &LivePoolCache,
    pool: &Pubkey,
) -> Option<PumpAmmSlaveRecoveryEvidence> {
    let (state, slot, _age_ms) = cache.get_with_metadata(pool)?;
    let CachedPoolState::PumpAmm(s) = state else {
        return None;
    };
    let (sell_extended, sell_third_meta, sell_tail_0, sell_tail_1) =
        cache.pump_amm_sell_extended_layout(pool);
    let sell_layout_ready = cache.pump_amm_sell_layout_ready(pool);
    let sell_requires_pre_fee_metas = cache.pump_amm_sell_requires_pre_fee_metas(pool);
    let sell_requires_fee_tail = cache.pump_amm_sell_requires_fee_tail(pool);
    let sell_ix_account_count =
        ironcrab::solana::dex::pumpfun_amm::pump_amm_inferred_sell_ix_account_count(
            sell_requires_pre_fee_metas,
            sell_requires_fee_tail,
            sell_extended,
        );
    Some((
        slot,
        s.base_reserve,
        s.quote_reserve,
        s.pool_accounts.len(),
        sell_extended,
        sell_third_meta,
        sell_tail_0,
        sell_tail_1,
        sell_layout_ready,
        sell_requires_pre_fee_metas,
        sell_ix_account_count,
        cache.pump_amm_sell_pre_fee_meta_1(pool),
        cache.pump_amm_layout_generation(pool),
    ))
}

/// Structured timeout log for PumpSwap liquidation / cold-path SLAVE wait (P184m).
fn log_pump_amm_slave_wait_timeout_evidence(
    mint: &Pubkey,
    pool: &str,
    cache: &LivePoolCache,
    timeout_ms: u64,
) {
    let Some(pool_pk) = Pubkey::from_str(pool).ok() else {
        warn!(
            mint = %mint,
            pool = %pool,
            timeout_ms,
            "pump_amm: usable cache state not visible after discovery timeout (invalid pool pubkey)"
        );
        return;
    };
    if let Some((
        slot,
        base_r,
        quote_r,
        acct_len,
        sell_extended,
        _sell_third,
        _tail0,
        _tail1,
        sell_layout_ready,
        sell_pre_fee,
        sell_ix_count,
        _pre_fee_meta_1,
        layout_gen,
    )) = pump_amm_slave_recovery_snapshot(cache, &pool_pk)
    {
        warn!(
            mint = %mint,
            pool = %pool,
            timeout_ms,
            cache_slot = slot,
            base_reserve = ?base_r,
            quote_reserve = ?quote_r,
            pool_accounts_len = acct_len,
            sell_extended,
            sell_layout_ready,
            sell_requires_pre_fee_metas = sell_pre_fee,
            sell_ix_account_count = sell_ix_count,
            layout_generation = layout_gen,
            "pump_amm: usable cache state not visible after discovery timeout (pool_accounts+nonzero reserves)"
        );
    } else {
        warn!(
            mint = %mint,
            pool = %pool,
            timeout_ms,
            "pump_amm: usable cache state not visible after discovery timeout (no PumpAmm SLAVE row)"
        );
    }
}

fn pump_amm_slave_recovery_evidence_changed(
    before: &PumpAmmSlaveRecoveryEvidence,
    after: &PumpAmmSlaveRecoveryEvidence,
) -> bool {
    before.0 != after.0
        || before.1 != after.1
        || before.2 != after.2
        || before.3 != after.3
        || before.4 != after.4
        || before.5 != after.5
        || before.6 != after.6
        || before.7 != after.7
        || before.8 != after.8
        || before.9 != after.9
        || before.10 != after.10
        || before.11 != after.11
        || before.12 != after.12
}

/// After `EnsurePumpAmmPoolAccounts(force_refresh=true)` + JetStream merge: bounded wait until
/// the SLAVE cache shows this **specific** PumpSwap pool as explicitly JetStream-ready with a
/// **fresh** snapshot vs `before`.
///
/// This avoids mint-level false positives where another pool for the same mint is ready, and
/// blocks stale pre-existing ready rows from satisfying recovery immediately (Bug #34 / #36).
async fn wait_for_pump_amm_slave_after_recovery(
    cache: &LivePoolCache,
    pool: &Pubkey,
    before: Option<PumpAmmSlaveRecoveryEvidence>,
    timeout_ms: u64,
    poll_interval_ms: u64,
) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        let explicit_ready = cache
            .get_explicit_jetstream_ready_pump_amm_pool_accounts_for_pool_market(pool)
            .is_some();
        if explicit_ready {
            let after = pump_amm_slave_recovery_snapshot(cache, pool);
            let layout_gen_fresh = before
                .as_ref()
                .zip(after.as_ref())
                .is_some_and(|(b, a)| a.12 > b.12);
            let evidence_changed = match (&before, &after) {
                (None, Some(_)) => true,
                (Some(b), Some(a)) => pump_amm_slave_recovery_evidence_changed(b, a),
                _ => false,
            };
            if after.is_some() && (before.is_none() || evidence_changed || layout_gen_fresh) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }
    false
}

/// PumpSwap cold-path pre-plan gate: must match [`tx_builder::build_tx_plan`] when
/// `resources.accounts` is empty — it reads `cache.get(pool_market)` and requires ≥12 accounts
/// (SELL accepts 12 or 14). Do **not** require 14 here; that would block valid 12-account SELL
/// cache rows and misalign with the builder.
#[inline]
fn pump_amm_hint_pool_cache_usable_for_tx_plan_builder(
    cache: &LivePoolCache,
    pool_market_hint: &Pubkey,
) -> bool {
    matches!(
        cache.get(pool_market_hint),
        Some(CachedPoolState::PumpAmm(ref s)) if s.pool_accounts.len() >= 12
    )
}

/// I-24d: bounded wait until SLAVE cache exposes a PumpSwap row for `pool_market_hint` that
/// satisfies the same ≥12-account rule as [`tx_builder::build_tx_plan`] (empty intent accounts).
async fn wait_for_pump_amm_pool_hint_ready_for_tx_plan_builder(
    cache: &LivePoolCache,
    pool_market_hint: &Pubkey,
    timeout_ms: u64,
    poll_interval_ms: u64,
) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    while std::time::Instant::now() < deadline {
        if pump_amm_hint_pool_cache_usable_for_tx_plan_builder(cache, pool_market_hint) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }
    false
}

/// After `EnsureOrcaWhirlpoolPoolState` + JetStream merge: bounded wait until the SLAVE cache shows
/// explicit Orca [`DexPoolReadiness::Ready`] **and** a **fresh** Whirlpool evidence tuple vs `before`.
///
/// `ControlResponse::Ok` from market-data is correlated only after an Orca-`Ready` JetStream
/// publish; this wait still requires a changed snapshot so an older `Ready` cannot satisfy
/// immediately (Bug #34). Does not treat cache rows as ready without explicit merge (Bug #36).
async fn wait_for_orca_whirlpool_slave_after_recovery(
    cache: &LivePoolCache,
    pool: &Pubkey,
    before: Option<(i32, u128, u128, u64, u64)>,
    timeout_ms: u64,
    poll_interval_ms: u64,
) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        if let Some((s, merged)) = cache.orca_whirlpool_slave_readiness_snapshot(pool) {
            let after = (
                s.tick_current_index,
                s.sqrt_price,
                s.liquidity,
                s.vault_a_balance.unwrap_or(0),
                s.vault_b_balance.unwrap_or(0),
            );
            if merged == DexPoolReadiness::Ready && Some(after) != before {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }
    false
}

/// After `EnsureMeteoraDlmmPoolState` + JetStream merge: bounded wait until SLAVE shows explicit
/// Meteora DLMM [`DexPoolReadiness::Ready`] and a **fresh** evidence tuple vs `before`.
///
/// Callers must pass `before` from a snapshot taken **before** publishing the control request
/// (same contract as [`wait_for_orca_whirlpool_slave_after_recovery`]). Otherwise a fast merge can
/// make `before` equal the post-recovery state and this wait never observes a change (Bug #34).
///
/// `active_id == 0` with unchanged tuple does not satisfy liquidation/build needs (legacy skip);
/// successful wait requires explicit `Ready` and snapshot change (Bug #34 / #36).
async fn wait_for_meteora_dlmm_slave_after_recovery(
    cache: &LivePoolCache,
    pool: &Pubkey,
    before: Option<(i32, u16, Option<u64>, Option<u64>)>,
    timeout_ms: u64,
    poll_interval_ms: u64,
) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        if let Some((s, merged)) = cache.meteora_dlmm_slave_readiness_snapshot(pool) {
            let after = (
                s.active_id,
                s.bin_step,
                s.reserve_x_balance,
                s.reserve_y_balance,
            );
            if merged == DexPoolReadiness::Ready && Some(after) != before {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }
    false
}

/// Evidence tuple for Raydium AMM cold-path JetStream wait (Bug #34): reserves + serum triple.
type RaydiumAmmSlaveEvidence = (u64, u64, Option<Pubkey>, Option<Pubkey>, Option<Pubkey>);

/// After `EnsureRaydiumAmmPoolState` + JetStream merge: bounded wait until SLAVE shows explicit
/// Raydium AMM v4 [`DexPoolReadiness::Ready`] and a **fresh** evidence tuple vs `before`
/// (reserves + serum pubkey triple). Same Bug #34 / #36 contract as Orca / DLMM.
async fn wait_for_raydium_amm_slave_after_recovery(
    cache: &LivePoolCache,
    pool: &Pubkey,
    before: Option<RaydiumAmmSlaveEvidence>,
    timeout_ms: u64,
    poll_interval_ms: u64,
) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        if let Some((s, merged)) = cache.raydium_amm_slave_readiness_snapshot(pool) {
            let after = (
                s.coin_reserve.unwrap_or(0),
                s.pc_reserve.unwrap_or(0),
                s.serum_bids,
                s.serum_asks,
                s.serum_event_queue,
            );
            if merged == DexPoolReadiness::Ready && Some(after) != before {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }
    false
}

/// After `EnsurePumpfunBondingCurve` + JetStream merge: wait until SLAVE cache reflects a change
/// vs the pre-request snapshot (or new entry appears).
///
/// When `require_explicit_ready` is true (cold-path recovery after RPC refresh), we require both:
/// - [`LivePoolCache::pumpfun_bonding_curve_explicitly_ready`] (JetStream carried `Ready`), **and**
/// - a **fresh** bonding-curve snapshot vs `before` (proves this request’s merge, not a stale Ready).
///
/// Without the snapshot check, an older `Ready` + unchanged reserves could satisfy the wait
/// immediately after `ControlResponse::Ok` (Bug #34).
async fn wait_for_pumpfun_bonding_cache_refresh(
    cache: &LivePoolCache,
    bonding_curve: &Pubkey,
    before: Option<(u64, u64, u64, u64, bool, bool)>,
    timeout_ms: u64,
    poll_interval_ms: u64,
    require_explicit_ready: bool,
) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        let after = cache.pumpfun_bonding_curve_reserves_snapshot(bonding_curve);
        if require_explicit_ready {
            if cache.pumpfun_bonding_curve_explicitly_ready(bonding_curve)
                && after.is_some()
                && after != before
            {
                return true;
            }
        } else if after != before {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }
    false
}

/// Runtime context for execution-engine
struct ExecutionContext {
    run_id: String,
    rpc_url: String,
    #[allow(dead_code)]
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
    /// PA-2: passive PositionAuthority (metrics only; not used for gating or reservations).
    position_authority: Arc<ParkingMutex<PositionAuthority>>,
    /// PA-5.1 / PA-6b: FIFO queue for ordered PositionAuthority KV publishes (disabled when PM is writer).
    position_authority_kv_publisher: PositionAuthorityKvPublisher,
    log_base: PathBuf, // P1: For state persistence
    decision_counter: std::sync::atomic::AtomicU64,
    execution_counter: std::sync::atomic::AtomicU64,

    // === Risk Tracking (DoD J) P0) ===
    /// Current day (UTC) for daily loss tracking
    current_day: parking_lot::RwLock<chrono::NaiveDate>,
    /// Cumulative loss today (lamports, positive = loss)
    daily_loss_lamports: std::sync::atomic::AtomicI64,
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

    // === PR 5: Cached blockhash from Geyser (via market-data NATS) ===
    /// Latest confirmed blockhash from Geyser blocks_meta stream.
    /// Tuple: (blockhash, slot, block_height, received_at)
    /// Falls back to RPC if stale (> MAX_BLOCKHASH_AGE_SECS).
    cached_blockhash: parking_lot::RwLock<Option<CachedBlockhash>>,

    // === Option C: Live Pool Cache (P0: fresh quotes, no RPC in hot path) ===
    /// Cache of pool states from Geyser for fresh quote calculation
    live_pool_cache: Option<Arc<ironcrab::execution::live_pool_cache::LivePoolCache>>,

    // === FIX-31: Parallel Intent Processing ===
    /// Semaphore limiting how many intents run concurrently (sized from config at startup)
    intent_semaphore: Arc<tokio::sync::Semaphore>,

    /// I-24d: Pending Discovery Request/Reply correlation (request_id -> oneshot sender).
    pending_discovery_responses: Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, tokio::sync::oneshot::Sender<ControlResponse>>,
        >,
    >,

    /// Channel to send SOL/WSOL balance updates to WsolManager (from JetStream consumer).
    /// When Some, WalletBalanceSnapshot for NATIVE_SOL/WSOL are forwarded here.
    wsol_balance_tx: Option<tokio::sync::mpsc::Sender<(u64, Option<u64>)>>,

    /// Shared with WsolManager: pending-wrap floor for LockManager capital sync.
    wsol_pending_wrap: Option<Arc<PendingWrapState>>,

    // === PR3: JetStream TX Confirmation ===
    /// Pending signature → oneshot notify (filled by main-loop JetStream WalletTxConfirmed consumer).
    pending_tx_confirms: Arc<
        parking_lot::RwLock<
            std::collections::HashMap<String, tokio::sync::oneshot::Sender<WalletTxConfirmNotify>>,
        >,
    >,
    /// PR3.1: Confirms that arrived before the intent waiter was registered (main-loop race).
    recent_orphan_tx_confirms:
        Arc<parking_lot::RwLock<std::collections::HashMap<String, OrphanTxConfirmEntry>>>,

    // Metrics
    intents_received: std::sync::atomic::AtomicU64,
    intents_rejected: std::sync::atomic::AtomicU64,
    sim_failures: std::sync::atomic::AtomicU64,
    #[allow(dead_code)]
    tx_sent: std::sync::atomic::AtomicU64,
    arb_validated: std::sync::atomic::AtomicU64,
    #[allow(dead_code)]
    arb_executed: std::sync::atomic::AtomicU64,

    /// Golden Replay Mode: true = no RPC/TX send, early-exit for SIMFAIL intents.
    replay_mode: bool,

    /// Scope-2: last `Instant` a hot-path async `EnsurePumpAmmPoolAccounts` was **successfully** published
    /// (`nats.publish` → `Ok(true)`) per base mint. Dedupes within [`PUMP_AMM_HOT_PATH_REFRESH_COOLDOWN`].
    /// No RPC; in-memory only.
    pump_amm_hot_path_refresh_last: Arc<ParkingMutex<HashMap<Pubkey, Instant>>>,
}

#[cfg(test)]
impl ExecutionContext {
    fn test_for_pa2_metrics(
        lock_manager: LockManager,
        position_authority: PositionAuthority,
    ) -> Self {
        use ironcrab::storage::JsonlWriterConfig;
        let log_dir =
            std::env::temp_dir().join(format!("ironcrab_ee_pa2_metrics_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&log_dir);
        let decision_writer = JsonlWriter::new(
            JsonlWriterConfig::new("test_decisions").with_log_dir(log_dir.join("decisions")),
        )
        .expect("decision writer");
        let execution_writer = JsonlWriter::new(
            JsonlWriterConfig::new("test_executions").with_log_dir(log_dir.join("executions")),
        )
        .expect("execution writer");
        let burn_writer = JsonlWriter::new(
            JsonlWriterConfig::new("test_burns").with_log_dir(log_dir.join("burns")),
        )
        .expect("burn writer");
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        Self {
            run_id: "test".to_string(),
            rpc_url: "test".to_string(),
            helius_rpc_url: None,
            wallet_pubkey: None,
            treasury: None,
            config: parking_lot::RwLock::new(ExecutionConfig::default()),
            config_snapshot_id: parking_lot::RwLock::new("test".to_string()),
            nats: None,
            decision_writer,
            execution_writer,
            burn_writer,
            lock_manager,
            position_authority: Arc::new(ParkingMutex::new(position_authority)),
            position_authority_kv_publisher: PositionAuthorityKvPublisher::disabled(),
            log_base: log_dir,
            decision_counter: std::sync::atomic::AtomicU64::new(0),
            execution_counter: std::sync::atomic::AtomicU64::new(0),
            current_day: parking_lot::RwLock::new(chrono::Utc::now().date_naive()),
            daily_loss_lamports: std::sync::atomic::AtomicI64::new(0),
            kill_switch_active: AtomicBool::new(false),
            liquidation_in_progress: AtomicBool::new(false),
            kill_switch_context: parking_lot::RwLock::new(None),
            burn_in_progress: AtomicBool::new(false),
            jito_client: None,
            bundles_submitted: std::sync::atomic::AtomicU64::new(0),
            bundles_confirmed: std::sync::atomic::AtomicU64::new(0),
            cross_dex_handler: None,
            rpc,
            address_lookup_table: None,
            tx_sender: None,
            dynamic_fee_percentiles: parking_lot::RwLock::new(None),
            cached_blockhash: parking_lot::RwLock::new(None),
            live_pool_cache: None,
            intent_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            pending_discovery_responses: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            wsol_balance_tx: None,
            wsol_pending_wrap: None,
            pending_tx_confirms: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            recent_orphan_tx_confirms: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            intents_received: std::sync::atomic::AtomicU64::new(0),
            intents_rejected: std::sync::atomic::AtomicU64::new(0),
            sim_failures: std::sync::atomic::AtomicU64::new(0),
            tx_sent: std::sync::atomic::AtomicU64::new(0),
            arb_validated: std::sync::atomic::AtomicU64::new(0),
            arb_executed: std::sync::atomic::AtomicU64::new(0),
            replay_mode: true,
            pump_amm_hot_path_refresh_last: Arc::new(ParkingMutex::new(HashMap::new())),
        }
    }
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
    /// Blockhash + slot for TX signing. Slot is always paired with the hash we sign with.
    /// Falls back to RPC if Geyser cache is empty/stale and refreshes `cached_blockhash`.
    async fn get_latest_blockhash_for_send(&self) -> Result<BlockhashForSend, String> {
        if let Some(ref cached) = *self.cached_blockhash.read() {
            let age_secs = cached.received_at.elapsed().as_secs();
            if age_secs <= MAX_BLOCKHASH_AGE_SECS {
                return Ok(BlockhashForSend {
                    hash: cached.hash,
                    slot: cached.slot,
                });
            }
            warn!(
                age_secs,
                slot = cached.slot,
                "cached blockhash too old, falling back to RPC"
            );
        }

        warn!("BLOCKHASH_SOURCE_RPC_FALLBACK: no fresh Geyser blockhash, using RPC");
        let hash = self
            .rpc
            .get_latest_blockhash_retry()
            .await
            .map_err(|e| format!("rpc_error:{e}"))?;
        let slot = self
            .rpc
            .rpc
            .get_slot()
            .await
            .map_err(|e| format!("rpc_error:{e}"))?;
        let received_at = std::time::Instant::now();
        apply_rpc_blockhash_cache_fallback(
            &mut self.cached_blockhash.write(),
            hash,
            slot,
            received_at,
        );
        Ok(BlockhashForSend { hash, slot })
    }

    /// Get a recent blockhash, preferring the Geyser-cached version.
    /// Falls back to RPC if cache is empty or stale (> MAX_BLOCKHASH_AGE_SECS).
    async fn get_latest_blockhash(&self) -> Result<solana_sdk::hash::Hash, String> {
        Ok(self.get_latest_blockhash_for_send().await?.hash)
    }

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

    fn get_priority_fee_for_intent(
        &self,
        intent: &TradeIntent,
        fee_policy: &FeePolicy,
    ) -> PriorityFeeSelection {
        let percentiles = self.dynamic_fee_percentiles.read();
        select_priority_fee_for_intent(intent, fee_policy, percentiles.as_ref())
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

        if let Some(dex) = dex_hint {
            if dex == "pumpfun" || dex == "pump_amm" {
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

    /// Cold-path: build a PumpFun bonding-curve SELL quote. `route_label` is `pumpfun_preferred`
    /// (Scope 51: try before multi-pool) or `pumpfun_fallback` (legacy: last resort after multi-pool).
    #[allow(clippy::too_many_arguments)] // Consolidates duplicated liquidation pumpfun SELL build path
    async fn liquidation_build_pumpfun_sell(
        &self,
        pumpfun: &PumpFunDex,
        mint: &Pubkey,
        sol_mint: &Pubkey,
        amount_in: u64,
        max_slippage_bps: u32,
        route_label: &str,
        metadata: &mut HashMap<String, String>,
        resources: &mut TradeResources,
        quote_attempts: &mut Vec<String>,
    ) -> Option<u64> {
        let mut min_out: Option<u64> = None;
        match pumpfun
            .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), amount_in)
            .await
        {
            Ok(Some(q)) => {
                if let Some(bc_str) = q.route.first() {
                    resources.pools = vec![bc_str.clone()];

                    let mut _creator_found = false;
                    if let Ok(bc) = Pubkey::from_str(bc_str) {
                        if let Some(cache) = self.live_pool_cache.as_ref() {
                            if let Some(creator) = cache.get_pumpfun_creator(&bc) {
                                metadata.insert("creator".to_string(), creator.to_string());
                                _creator_found = true;
                            }
                        }

                        if !_creator_found {
                            warn!(
                                mint = %mint,
                                bonding_curve = %bc,
                                "LIQUIDATION: Creator not in cache, falling back to RPC"
                            );
                            match self.rpc.get_account(&bc).await {
                                Ok(account) => {
                                    if account.data.len() >= 81 {
                                        let creator_bytes: [u8; 32] = account.data[49..81]
                                            .try_into()
                                            .expect("slice is exactly 32 bytes");
                                        let creator = Pubkey::new_from_array(creator_bytes);
                                        if creator != Pubkey::default() {
                                            metadata
                                                .insert("creator".to_string(), creator.to_string());
                                            _creator_found = true;
                                            info!(
                                                mint = %mint,
                                                creator = %creator,
                                                "LIQUIDATION: Creator fetched via RPC fallback"
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        error = %e,
                                        mint = %mint,
                                        "LIQUIDATION: RPC fallback for creator failed"
                                    );
                                }
                            }
                        }
                    }
                    if metadata.contains_key("creator") && resources.pools.len() == 1 {
                        metadata.insert("sell_routing".to_string(), route_label.to_string());
                        metadata.insert("dex".to_string(), "pumpfun".to_string());
                        min_out =
                            Some(Self::apply_slippage_min_out(q.amount_out, max_slippage_bps));
                        quote_attempts.push(format!(
                            "{}=ok amount_out={} route={}",
                            route_label,
                            q.amount_out,
                            resources
                                .pools
                                .first()
                                .map(|s| s.as_str())
                                .unwrap_or("<none>")
                        ));
                    } else {
                        quote_attempts.push(format!(
                            "{}=skip missing_creator_or_pool creator_present={} pools_len={}",
                            route_label,
                            metadata.contains_key("creator"),
                            resources.pools.len()
                        ));
                    }
                } else {
                    quote_attempts.push(format!("{route_label}=none empty_route"));
                }
            }
            Ok(None) => {
                quote_attempts.push(format!("{route_label}=none"));
            }
            Err(e) => {
                quote_attempts.push(format!("{route_label}=err {e:#}"));
            }
        }
        min_out
    }

    /// 6005-Retry: Bei BondingCurveComplete (PumpFun) Retry mit PumpSwap AMM.
    /// Wird von run_liquidation_job und run_golden_replay genutzt.
    async fn try_6005_pumpfun_retry(
        &self,
        intent: &TradeIntent,
        pump_amm: &PumpFunAmmDex,
        max_slippage_bps: u32,
    ) -> Option<TradeIntent> {
        let mint_str = intent.resources.input_mint.clone();
        let sol_mint_str = if intent.resources.output_mint == "SIMFAIL6005" {
            "So11111111111111111111111111111111111111112".to_string()
        } else {
            intent.resources.output_mint.clone()
        };
        let amount_in = intent.required_capital.raw;
        let mint = match Pubkey::from_str(&mint_str) {
            Ok(m) => m,
            Err(e) => {
                warn!(mint = %mint_str, error = %e, "6005-retry: invalid base_mint");
                return None;
            }
        };

        if let Some(ref cache) = self.live_pool_cache {
            cache.mark_pumpfun_complete_for_mint(&mint);
        }

        let pump_amm_result = tokio::time::timeout(
            Duration::from_secs(PUMPSWAP_LIQUIDATION_QUOTE_TIMEOUT_SECS),
            pump_amm.quote_exact_in(&mint_str, &sol_mint_str, amount_in),
        )
        .await;

        match pump_amm_result {
            Ok(Ok(Some(ref q))) => {
                let pool_id = match q.route.first() {
                    Some(id) => id.clone(),
                    None => {
                        warn!(mint = %mint_str, "6005-retry: quote route empty");
                        return None;
                    }
                };
                // I-24d: Cache-only — no pump_amm.pool_accounts_v1_for_base_mint (would trigger RPC).
                let accounts = match self
                    .live_pool_cache
                    .as_ref()
                    .and_then(|c| c.get_ready_pump_amm_pool_accounts_by_base_mint(&mint))
                {
                    Some(a) if a.len() >= 14 => a,
                    _ => {
                        // I-24d: Request from market-data, wait bounded on authoritative cache state.
                        let pool_hint = self
                            .live_pool_cache
                            .as_ref()
                            .and_then(|c| c.get_pump_amm_pool_address_by_base_mint(&mint))
                            .map(|p| p.to_string());
                        info!(mint = %mint_str, pool_address = ?pool_hint, "6005-retry: pool_accounts cache miss, requesting discovery from market-data");
                        match self
                            .request_discovery_and_wait(
                                &mint_str,
                                pool_hint.as_deref(),
                                pump_amm_liquidation_discovery_force_refresh(),
                            )
                            .await
                        {
                            DiscoveryRequestOutcome::Ok => {
                                let cache = match self.live_pool_cache.as_ref() {
                                    Some(c) => c,
                                    None => {
                                        warn!(mint = %mint_str, "6005-retry: no cache");
                                        return None;
                                    }
                                };
                                if !wait_for_usable_pump_amm_cache_state(
                                    cache,
                                    &mint,
                                    PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                                    DISCOVERY_CACHE_POLL_INTERVAL_MS,
                                )
                                .await
                                {
                                    warn!(mint = %mint_str, "6005-retry: usable PumpAmm cache state not visible after discovery timeout (pool_accounts+reserves)");
                                    return None;
                                }
                                match cache.get_ready_pump_amm_pool_accounts_by_base_mint(&mint) {
                                    Some(a) if a.len() >= 14 => a,
                                    _ => {
                                        warn!(mint = %mint_str, "6005-retry: pool_accounts still missing after discovery");
                                        return None;
                                    }
                                }
                            }
                            DiscoveryRequestOutcome::NotFound => {
                                warn!(mint = %mint_str, "6005-retry: discovery not_found");
                                return None;
                            }
                            DiscoveryRequestOutcome::Error(e) => {
                                warn!(mint = %mint_str, error = %e, "6005-retry: discovery error");
                                return None;
                            }
                            DiscoveryRequestOutcome::Timeout => {
                                warn!(mint = %mint_str, "6005-retry: discovery timeout");
                                return None;
                            }
                        }
                    }
                };
                let acct_strings: Vec<String> = accounts.iter().map(|p| p.to_string()).collect();
                let min_out = Self::apply_slippage_min_out(q.amount_out, max_slippage_bps);
                let mut retry = intent.clone();
                retry
                    .metadata
                    .insert("dex".to_string(), "pump_amm".to_string());
                retry
                    .metadata
                    .insert("sell_routing".to_string(), "multi_pool".to_string());
                retry
                    .metadata
                    .insert("6005_retry".to_string(), "true".to_string());
                retry.resources.pools = vec![pool_id];
                retry.resources.accounts = acct_strings;
                retry.execution = Some(TradeExecutionConstraints {
                    min_out: Some(ExplicitAmount::new(min_out, 9)),
                });
                Some(retry)
            }
            Ok(Ok(None)) => {
                // I-24d: Cold-Path Recovery — quote cache miss/degenerate, trigger Discovery-Request.
                info!(
                    mint = %mint_str,
                    "6005-retry: quote cache miss, requesting discovery from market-data"
                );
                let pool_hint = self
                    .live_pool_cache
                    .as_ref()
                    .and_then(|c| c.get_pump_amm_pool_address_by_base_mint(&mint))
                    .map(|p| p.to_string());
                match self
                    .request_discovery_and_wait(
                        &mint_str,
                        pool_hint.as_deref(),
                        pump_amm_liquidation_discovery_force_refresh(),
                    )
                    .await
                {
                    DiscoveryRequestOutcome::Ok => {
                        let cache = match self.live_pool_cache.as_ref() {
                            Some(c) => c,
                            None => {
                                warn!(mint = %mint_str, "6005-retry: no cache");
                                return None;
                            }
                        };
                        if !wait_for_usable_pump_amm_cache_state(
                            cache,
                            &mint,
                            PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                            DISCOVERY_CACHE_POLL_INTERVAL_MS,
                        )
                        .await
                        {
                            warn!(
                                mint = %mint_str,
                                timeout_ms = PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                                "6005-retry: usable PumpAmm cache state not visible after discovery timeout (pool_accounts+reserves)"
                            );
                            return None;
                        }
                        // Retry quote_exact_in (cache may now have reserves + pool_accounts).
                        let retry_quote = pump_amm
                            .quote_exact_in(&mint_str, &sol_mint_str, amount_in)
                            .await;
                        match retry_quote {
                            Ok(Some(ref q)) => {
                                let pool_id = match q.route.first() {
                                    Some(id) => id.clone(),
                                    None => {
                                        warn!(mint = %mint_str, "6005-retry: retry quote route empty");
                                        return None;
                                    }
                                };
                                let accounts = match cache
                                    .get_ready_pump_amm_pool_accounts_by_base_mint(&mint)
                                {
                                    Some(a) if a.len() >= 14 => a,
                                    _ => {
                                        warn!(mint = %mint_str, "6005-retry: pool_accounts missing after retry");
                                        return None;
                                    }
                                };
                                let acct_strings: Vec<String> =
                                    accounts.iter().map(|p| p.to_string()).collect();
                                let min_out =
                                    Self::apply_slippage_min_out(q.amount_out, max_slippage_bps);
                                let mut retry = intent.clone();
                                retry
                                    .metadata
                                    .insert("dex".to_string(), "pump_amm".to_string());
                                retry
                                    .metadata
                                    .insert("sell_routing".to_string(), "multi_pool".to_string());
                                retry
                                    .metadata
                                    .insert("6005_retry".to_string(), "true".to_string());
                                retry.resources.pools = vec![pool_id];
                                retry.resources.accounts = acct_strings;
                                retry.execution = Some(TradeExecutionConstraints {
                                    min_out: Some(ExplicitAmount::new(min_out, 9)),
                                });
                                Some(retry)
                            }
                            _ => {
                                warn!(
                                    mint = %mint_str,
                                    "6005-retry: quote still None after discovery (reserves may be degenerate)"
                                );
                                None
                            }
                        }
                    }
                    DiscoveryRequestOutcome::NotFound => {
                        warn!(mint = %mint_str, "6005-retry: discovery not_found");
                        None
                    }
                    DiscoveryRequestOutcome::Error(e) => {
                        warn!(mint = %mint_str, error = %e, "6005-retry: discovery error");
                        None
                    }
                    DiscoveryRequestOutcome::Timeout => {
                        warn!(mint = %mint_str, "6005-retry: discovery timeout");
                        None
                    }
                }
            }
            Ok(Err(e)) => {
                warn!(mint = %mint_str, error = %e, "6005-retry: quote_exact_in failed");
                None
            }
            Err(_) => {
                warn!(mint = %mint_str, "6005-retry: quote_exact_in timeout");
                None
            }
        }
    }

    /// I-24d: Request PumpSwap pool_accounts discovery from market-data (Cold Path only).
    /// Sends EnsurePumpAmmPoolAccounts, waits bounded for ControlResponse, returns outcome.
    /// Does NOT write to SLAVE cache — market-data publishes to JetStream, SLAVE consumes.
    /// When pool_address is provided, market-data uses fast getAccount path (<1s) instead of slow getProgramAccounts.
    /// `force_refresh`: market-data must re-resolve pool_accounts via RPC (not cache-first 14er set).
    async fn request_discovery_and_wait(
        &self,
        base_mint: &str,
        pool_address: Option<&str>,
        force_refresh: bool,
    ) -> DiscoveryRequestOutcome {
        let Some(ref nats) = self.nats else {
            warn!(
                request_id = "n/a",
                base_mint = %base_mint,
                "I-24d Discovery: NATS not connected"
            );
            return DiscoveryRequestOutcome::Error("NATS not connected".to_string());
        };

        let request_id = Uuid::new_v4().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();

        {
            let mut pending = self.pending_discovery_responses.lock().await;
            pending.insert(request_id.clone(), tx);
        }

        let mut req = ControlRequest::new(
            "execution-engine",
            BUILD_VERSION,
            &self.run_id,
            request_id.clone(),
            "market-data",
            ControlRequestKind::EnsurePumpAmmPoolAccounts {
                base_mint: base_mint.to_string(),
            },
        );
        req.pool_address_hint = pool_address.map(String::from);
        req.force_refresh = force_refresh;

        info!(
            request_id = %request_id,
            base_mint = %base_mint,
            target = "market-data",
            pool_address = ?pool_address,
            force_refresh = %force_refresh,
            "I-24d Discovery: publishing EnsurePumpAmmPoolAccounts request"
        );

        if nats.publish(TOPIC_CONTROL_REQUESTS, &req).await.is_err() {
            let mut pending = self.pending_discovery_responses.lock().await;
            pending.remove(&request_id);
            warn!(
                request_id = %request_id,
                base_mint = %base_mint,
                "I-24d Discovery: publish failed"
            );
            return DiscoveryRequestOutcome::Error("publish failed".to_string());
        }

        let outcome =
            match tokio::time::timeout(Duration::from_secs(DISCOVERY_REQUEST_TIMEOUT_SECS), rx)
                .await
            {
                Ok(Ok(resp)) => {
                    let status_str = format!("{:?}", resp.status);
                    let pool = resp.pool_address.as_deref();
                    info!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        status = %status_str,
                        pool_address = ?pool,
                        "I-24d Discovery: response received and correlated"
                    );
                    match resp.status {
                        ControlResponseStatus::Ok => DiscoveryRequestOutcome::Ok,
                        ControlResponseStatus::NotFound => DiscoveryRequestOutcome::NotFound,
                        ControlResponseStatus::Busy => DiscoveryRequestOutcome::Timeout,
                        ControlResponseStatus::Error => DiscoveryRequestOutcome::Error(
                            resp.message.unwrap_or_else(|| "unknown error".to_string()),
                        ),
                    }
                }
                Ok(Err(_)) => {
                    warn!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        "I-24d Discovery: channel closed before response"
                    );
                    DiscoveryRequestOutcome::Error("channel closed".to_string())
                }
                Err(_) => {
                    warn!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        timeout_secs = DISCOVERY_REQUEST_TIMEOUT_SECS,
                        "I-24d Discovery: timeout (no correlated response)"
                    );
                    DiscoveryRequestOutcome::Timeout
                }
            };

        // Cleanup pending entry (may have been removed by subscription)
        let _ = self
            .pending_discovery_responses
            .lock()
            .await
            .remove(&request_id);

        outcome
    }

    /// I-24d: Request PumpFun bonding curve RPC refresh from market-data (Cold Path only).
    /// Sends EnsurePumpfunBondingCurve with `force_refresh_pumpfun=true`, waits bounded for ControlResponse.
    /// Does NOT write to SLAVE cache — market-data publishes PoolCacheUpdate to JetStream.
    async fn request_pumpfun_bonding_recovery_and_wait(
        &self,
        base_mint: &str,
    ) -> DiscoveryRequestOutcome {
        let Some(ref nats) = self.nats else {
            warn!(
                request_id = "n/a",
                base_mint = %base_mint,
                "PumpFun bonding recovery: NATS not connected"
            );
            return DiscoveryRequestOutcome::Error("NATS not connected".to_string());
        };

        let request_id = Uuid::new_v4().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();

        {
            let mut pending = self.pending_discovery_responses.lock().await;
            pending.insert(request_id.clone(), tx);
        }

        let mut req = ControlRequest::new(
            "execution-engine",
            BUILD_VERSION,
            &self.run_id,
            request_id.clone(),
            "market-data",
            ControlRequestKind::EnsurePumpfunBondingCurve {
                base_mint: base_mint.to_string(),
            },
        );
        req.force_refresh_pumpfun = true;

        warn!(
            request_id = %request_id,
            base_mint = %base_mint,
            bonding_curve = "derived PDA from mint (EnsurePumpfunBondingCurve)",
            error_family = "6023/6024/Overflow/0x1787/0x1788 (structural sim fail)",
            target = "market-data",
            "PumpFun cold-path: triggering EnsurePumpfunBondingCurve force_refresh via market-data (RPC, not cache-first)"
        );

        if nats.publish(TOPIC_CONTROL_REQUESTS, &req).await.is_err() {
            let mut pending = self.pending_discovery_responses.lock().await;
            pending.remove(&request_id);
            warn!(
                request_id = %request_id,
                base_mint = %base_mint,
                "PumpFun bonding recovery: publish failed"
            );
            return DiscoveryRequestOutcome::Error("publish failed".to_string());
        }

        let outcome =
            match tokio::time::timeout(Duration::from_secs(DISCOVERY_REQUEST_TIMEOUT_SECS), rx)
                .await
            {
                Ok(Ok(resp)) => {
                    let status_str = format!("{:?}", resp.status);
                    let pool = resp.pool_address.as_deref();
                    info!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        status = %status_str,
                        pool_address = ?pool,
                        "PumpFun bonding recovery: ControlResponse received"
                    );
                    match resp.status {
                        ControlResponseStatus::Ok => DiscoveryRequestOutcome::Ok,
                        ControlResponseStatus::NotFound => DiscoveryRequestOutcome::NotFound,
                        ControlResponseStatus::Busy => DiscoveryRequestOutcome::Timeout,
                        ControlResponseStatus::Error => DiscoveryRequestOutcome::Error(
                            resp.message.unwrap_or_else(|| "unknown error".to_string()),
                        ),
                    }
                }
                Ok(Err(_)) => {
                    warn!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        "PumpFun bonding recovery: channel closed before response"
                    );
                    DiscoveryRequestOutcome::Error("channel closed".to_string())
                }
                Err(_) => {
                    warn!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        timeout_secs = DISCOVERY_REQUEST_TIMEOUT_SECS,
                        "PumpFun bonding recovery: timeout (no correlated response)"
                    );
                    DiscoveryRequestOutcome::Timeout
                }
            };

        let _ = self
            .pending_discovery_responses
            .lock()
            .await
            .remove(&request_id);

        outcome
    }

    /// I-24d Scope 20: Orca Whirlpool cold-path recovery — `EnsureOrcaWhirlpoolPoolState` with
    /// `force_refresh_orca=true`. Does not write SLAVE locally; market-data publishes JetStream.
    async fn request_orca_whirlpool_recovery_and_wait(
        &self,
        base_mint: &str,
        pool_address_hint: Option<&str>,
    ) -> DiscoveryRequestOutcome {
        let Some(ref nats) = self.nats else {
            warn!(
                request_id = "n/a",
                base_mint = %base_mint,
                "Orca cold-path recovery: NATS not connected"
            );
            return DiscoveryRequestOutcome::Error("NATS not connected".to_string());
        };

        let request_id = Uuid::new_v4().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();

        {
            let mut pending = self.pending_discovery_responses.lock().await;
            pending.insert(request_id.clone(), tx);
        }

        let mut req = ControlRequest::new(
            "execution-engine",
            BUILD_VERSION,
            &self.run_id,
            request_id.clone(),
            "market-data",
            ControlRequestKind::EnsureOrcaWhirlpoolPoolState {
                base_mint: base_mint.to_string(),
            },
        );
        req.pool_address_hint = pool_address_hint.map(String::from);
        req.force_refresh_orca = true;

        warn!(
            request_id = %request_id,
            base_mint = %base_mint,
            pool_address_hint = ?pool_address_hint,
            target = "market-data",
            reason = "orca liquidation/manual sell sim structural fail — requesting authoritative Orca RPC refresh via market-data (I-24d)",
            "Orca cold-path: publishing EnsureOrcaWhirlpoolPoolState (force_refresh_orca)"
        );

        if nats.publish(TOPIC_CONTROL_REQUESTS, &req).await.is_err() {
            let mut pending = self.pending_discovery_responses.lock().await;
            pending.remove(&request_id);
            warn!(
                request_id = %request_id,
                base_mint = %base_mint,
                "Orca cold-path recovery: publish failed"
            );
            return DiscoveryRequestOutcome::Error("publish failed".to_string());
        }

        let outcome =
            match tokio::time::timeout(Duration::from_secs(DISCOVERY_REQUEST_TIMEOUT_SECS), rx)
                .await
            {
                Ok(Ok(resp)) => {
                    let status_str = format!("{:?}", resp.status);
                    let pool = resp.pool_address.as_deref();
                    info!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        status = %status_str,
                        pool_address = ?pool,
                        "Orca cold-path recovery: ControlResponse correlated"
                    );
                    match resp.status {
                        ControlResponseStatus::Ok => DiscoveryRequestOutcome::Ok,
                        ControlResponseStatus::NotFound => DiscoveryRequestOutcome::NotFound,
                        ControlResponseStatus::Busy => DiscoveryRequestOutcome::Timeout,
                        ControlResponseStatus::Error => DiscoveryRequestOutcome::Error(
                            resp.message.unwrap_or_else(|| "unknown error".to_string()),
                        ),
                    }
                }
                Ok(Err(_)) => {
                    warn!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        "Orca cold-path recovery: channel closed before response"
                    );
                    DiscoveryRequestOutcome::Error("channel closed".to_string())
                }
                Err(_) => {
                    warn!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        timeout_secs = DISCOVERY_REQUEST_TIMEOUT_SECS,
                        "Orca cold-path recovery: timeout waiting for ControlResponse"
                    );
                    DiscoveryRequestOutcome::Timeout
                }
            };

        let _ = self
            .pending_discovery_responses
            .lock()
            .await
            .remove(&request_id);

        outcome
    }

    /// I-24d Scope 21: Meteora DLMM cold-path recovery — `EnsureMeteoraDlmmPoolState` with
    /// `force_refresh_meteora_dlmm=true`. Does not write SLAVE locally; market-data publishes JetStream.
    async fn request_meteora_dlmm_recovery_and_wait(
        &self,
        base_mint: &str,
        pool_address_hint: Option<&str>,
    ) -> DiscoveryRequestOutcome {
        let Some(ref nats) = self.nats else {
            warn!(
                request_id = "n/a",
                base_mint = %base_mint,
                "Meteora DLMM cold-path recovery: NATS not connected"
            );
            return DiscoveryRequestOutcome::Error("NATS not connected".to_string());
        };

        let request_id = Uuid::new_v4().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();

        {
            let mut pending = self.pending_discovery_responses.lock().await;
            pending.insert(request_id.clone(), tx);
        }

        let mut req = ControlRequest::new(
            "execution-engine",
            BUILD_VERSION,
            &self.run_id,
            request_id.clone(),
            "market-data",
            ControlRequestKind::EnsureMeteoraDlmmPoolState {
                base_mint: base_mint.to_string(),
            },
        );
        req.pool_address_hint = pool_address_hint.map(String::from);
        req.force_refresh_meteora_dlmm = true;

        warn!(
            request_id = %request_id,
            base_mint = %base_mint,
            pool_address_hint = ?pool_address_hint,
            target = "market-data",
            reason = "meteora_dlmm liquidation cold path — SLAVE missing usable DLMM state (active_id/reserves/readiness); requesting authoritative RPC refresh via market-data (I-24d)",
            "Meteora DLMM cold-path: publishing EnsureMeteoraDlmmPoolState (force_refresh_meteora_dlmm)"
        );

        if nats.publish(TOPIC_CONTROL_REQUESTS, &req).await.is_err() {
            let mut pending = self.pending_discovery_responses.lock().await;
            pending.remove(&request_id);
            warn!(
                request_id = %request_id,
                base_mint = %base_mint,
                "Meteora DLMM cold-path recovery: publish failed"
            );
            return DiscoveryRequestOutcome::Error("publish failed".to_string());
        }

        let outcome =
            match tokio::time::timeout(Duration::from_secs(DISCOVERY_REQUEST_TIMEOUT_SECS), rx)
                .await
            {
                Ok(Ok(resp)) => {
                    let status_str = format!("{:?}", resp.status);
                    let pool = resp.pool_address.as_deref();
                    info!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        status = %status_str,
                        pool_address = ?pool,
                        "Meteora DLMM cold-path recovery: ControlResponse correlated"
                    );
                    match resp.status {
                        ControlResponseStatus::Ok => DiscoveryRequestOutcome::Ok,
                        ControlResponseStatus::NotFound => DiscoveryRequestOutcome::NotFound,
                        ControlResponseStatus::Busy => DiscoveryRequestOutcome::Timeout,
                        ControlResponseStatus::Error => DiscoveryRequestOutcome::Error(
                            resp.message.unwrap_or_else(|| "unknown error".to_string()),
                        ),
                    }
                }
                Ok(Err(_)) => {
                    warn!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        "Meteora DLMM cold-path recovery: channel closed before response"
                    );
                    DiscoveryRequestOutcome::Error("channel closed".to_string())
                }
                Err(_) => {
                    warn!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        timeout_secs = DISCOVERY_REQUEST_TIMEOUT_SECS,
                        "Meteora DLMM cold-path recovery: timeout waiting for ControlResponse"
                    );
                    DiscoveryRequestOutcome::Timeout
                }
            };

        let _ = self
            .pending_discovery_responses
            .lock()
            .await
            .remove(&request_id);

        outcome
    }

    /// I-24d Scope 22: Raydium AMM v4 cold-path recovery — `EnsureRaydiumAmmPoolState` with
    /// `force_refresh_raydium_amm=true`. Does not write SLAVE locally; market-data publishes JetStream.
    async fn request_raydium_amm_recovery_and_wait(
        &self,
        base_mint: &str,
        pool_address_hint: Option<&str>,
    ) -> DiscoveryRequestOutcome {
        let Some(ref nats) = self.nats else {
            warn!(
                request_id = "n/a",
                base_mint = %base_mint,
                "Raydium AMM cold-path recovery: NATS not connected"
            );
            return DiscoveryRequestOutcome::Error("NATS not connected".to_string());
        };

        let request_id = Uuid::new_v4().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();

        {
            let mut pending = self.pending_discovery_responses.lock().await;
            pending.insert(request_id.clone(), tx);
        }

        let mut req = ControlRequest::new(
            "execution-engine",
            BUILD_VERSION,
            &self.run_id,
            request_id.clone(),
            "market-data",
            ControlRequestKind::EnsureRaydiumAmmPoolState {
                base_mint: base_mint.to_string(),
            },
        );
        req.pool_address_hint = pool_address_hint.map(String::from);
        req.force_refresh_raydium_amm = true;

        warn!(
            request_id = %request_id,
            base_mint = %base_mint,
            pool_address_hint = ?pool_address_hint,
            target = "market-data",
            reason = "liquidation/manual cold path — SLAVE has Raydium AMM rows for mint but no explicit Ready; requesting authoritative RPC refresh via market-data (I-24d)",
            "Raydium AMM cold-path: publishing EnsureRaydiumAmmPoolState (force_refresh_raydium_amm)"
        );

        if nats.publish(TOPIC_CONTROL_REQUESTS, &req).await.is_err() {
            let mut pending = self.pending_discovery_responses.lock().await;
            pending.remove(&request_id);
            warn!(
                request_id = %request_id,
                base_mint = %base_mint,
                "Raydium AMM cold-path recovery: publish failed"
            );
            return DiscoveryRequestOutcome::Error("publish failed".to_string());
        }

        let outcome =
            match tokio::time::timeout(Duration::from_secs(DISCOVERY_REQUEST_TIMEOUT_SECS), rx)
                .await
            {
                Ok(Ok(resp)) => {
                    let status_str = format!("{:?}", resp.status);
                    let pool = resp.pool_address.as_deref();
                    info!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        status = %status_str,
                        pool_address = ?pool,
                        "Raydium AMM cold-path recovery: ControlResponse correlated"
                    );
                    match resp.status {
                        ControlResponseStatus::Ok => DiscoveryRequestOutcome::Ok,
                        ControlResponseStatus::NotFound => DiscoveryRequestOutcome::NotFound,
                        ControlResponseStatus::Busy => DiscoveryRequestOutcome::Timeout,
                        ControlResponseStatus::Error => DiscoveryRequestOutcome::Error(
                            resp.message.unwrap_or_else(|| "unknown error".to_string()),
                        ),
                    }
                }
                Ok(Err(_)) => {
                    warn!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        "Raydium AMM cold-path recovery: channel closed before response"
                    );
                    DiscoveryRequestOutcome::Error("channel closed".to_string())
                }
                Err(_) => {
                    warn!(
                        request_id = %request_id,
                        base_mint = %base_mint,
                        timeout_secs = DISCOVERY_REQUEST_TIMEOUT_SECS,
                        "Raydium AMM cold-path recovery: timeout waiting for ControlResponse"
                    );
                    DiscoveryRequestOutcome::Timeout
                }
            };

        let _ = self
            .pending_discovery_responses
            .lock()
            .await
            .remove(&request_id);

        outcome
    }

    /// Hot-path healing: publish `EnsurePumpAmmPoolAccounts` to market-data with `force_refresh=true`
    /// without registering for ControlResponse (no wait, no retry in the same intent).
    /// RPC work happens only in market-data (Cold Path); this path is fire-and-forget NATS publish.
    /// On `Ok(true)` from publish, updates per-mint cooldown ([`record_pump_amm_hot_path_refresh_after_success`])
    /// when `base_mint_pk_for_cooldown` is `Some`.
    fn fire_pump_amm_pool_accounts_refresh_async(
        nats: &NatsClient,
        run_id: String,
        base_mint: String,
        pool_address_hint: Option<String>,
        cooldown_last: Arc<ParkingMutex<HashMap<Pubkey, Instant>>>,
        base_mint_pk_for_cooldown: Option<Pubkey>,
    ) {
        let nats = nats.clone_for_spawned_publish();
        tokio::spawn(async move {
            let request_id = Uuid::new_v4().to_string();
            let mut req = ControlRequest::new(
                "execution-engine",
                BUILD_VERSION,
                &run_id,
                request_id,
                "market-data",
                ControlRequestKind::EnsurePumpAmmPoolAccounts {
                    base_mint: base_mint.clone(),
                },
            );
            req.pool_address_hint = pool_address_hint.clone();
            req.force_refresh = true;

            match nats.publish(TOPIC_CONTROL_REQUESTS, &req).await {
                Ok(true) => {
                    PUMPSWAP_HOT_PATH_HEALING_ASYNC_PUBLISH_SUCCESS_TOTAL
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some(pk) = base_mint_pk_for_cooldown {
                        record_pump_amm_hot_path_refresh_after_success(
                            &cooldown_last,
                            pk,
                            Instant::now(),
                        );
                    }
                }
                Ok(false) => {
                    PUMPSWAP_HOT_PATH_HEALING_ASYNC_PUBLISH_FAIL_TOTAL
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        request_id = %req.request_id,
                        base_mint = %base_mint,
                        pool_address_hint = ?pool_address_hint,
                        "PumpSwap hot-path: async EnsurePumpAmmPoolAccounts publish dropped or failed (NATS)"
                    );
                }
                Err(e) => {
                    PUMPSWAP_HOT_PATH_HEALING_ASYNC_PUBLISH_FAIL_TOTAL
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        request_id = %req.request_id,
                        base_mint = %base_mint,
                        pool_address_hint = ?pool_address_hint,
                        error = %e,
                        "PumpSwap hot-path: async EnsurePumpAmmPoolAccounts publish error"
                    );
                }
            }
        });
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
        let pumpfun_cache = ctx.live_pool_cache.as_ref().map(Arc::clone);
        let pumpfun = match PumpFunDex::new(Arc::clone(&ctx.rpc), pumpfun_cache) {
            Ok(p) => Some(p),
            Err(e) => {
                warn!(error = %e, "Failed to init PumpFunDex; continuing with other DEXes");
                None
            }
        };
        let lpc = ctx
            .live_pool_cache
            .as_ref()
            .map(Arc::clone)
            .unwrap_or_else(create_shared_cache);
        // I-24d: allow_rpc_on_miss=false — no local PumpSwap discovery. Discovery only via
        // Request/Reply to market-data. Cache-only for quote and pool_accounts.
        let pump_amm = PumpFunAmmDex::new_with_cache(Arc::clone(&ctx.rpc), Arc::clone(&lpc), false);
        let mut meteora = MeteoraDlmm::new_with_live_cache(
            Arc::clone(&ctx.rpc),
            Some(lpc.clone()),
            false, // I-24d: no connector RPC fallback — Meteora DLMM cold-path refresh via market-data EnsureMeteoraDlmmPoolState
        );
        meteora.set_user_authority(owner);
        let raydium = Raydium::new_with_live_cache(
            Arc::clone(&ctx.rpc),
            Some(lpc.clone()),
            false, // I-24d: no connector RPC fallback — Raydium AMM cold-path via market-data EnsureRaydiumAmmPoolState
        );
        let orca = Orca::new_with_cache_ext(
            Arc::clone(&ctx.rpc),
            None,
            Some(lpc),
            true, // Liquidation = Cold Path — RPC fallback allowed
        );
        orca.set_user_authority(owner);

        if let Err(e) = orca.refresh_pools().await {
            warn!(error = %e, "Orca refresh_pools failed; liquidation may miss routes");
        }
        // Meteora + Orca: inject cached pools from Geyser LivePoolCache (NO RPC needed!)
        // The pools are already cached from market-data via Geyser subscription.
        if let Some(ref cache) = ctx.live_pool_cache {
            let mut meteora_count = 0;
            let mut orca_count = 0;
            let mut raydium_amm_count = 0;
            for (pool_addr, state) in cache.iter() {
                match state {
                    CachedPoolState::Meteora(ref ms) => {
                        if meteora.inject_cached_meteora_state(&pool_addr, ms).is_ok() {
                            meteora_count += 1;
                        }
                    }
                    CachedPoolState::Orca(ref os) => {
                        if orca.inject_cached_orca_state(&pool_addr, os).is_ok() {
                            orca_count += 1;
                        }
                    }
                    CachedPoolState::RaydiumAmm(ref s) => {
                        raydium.inject_cached_amm_state(
                            pool_addr,
                            s.base_mint,
                            s.quote_mint,
                            s.coin_vault,
                            s.pc_vault,
                            s.base_decimals,
                            s.quote_decimals,
                            s.coin_reserve,
                            s.pc_reserve,
                            s.market_id,
                            s.serum_bids,
                            s.serum_asks,
                            s.serum_event_queue,
                        );
                        raydium_amm_count += 1;
                    }
                    _ => {}
                }
            }
            info!(
                meteora_pools = meteora_count,
                orca_pools = orca_count,
                raydium_amm_pools = raydium_amm_count,
                "DEX pools injected from LivePoolCache (GEYSER-FIRST)"
            );
        }
        #[cfg(unix)]
        maybe_ping_watchdog();

        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();

        // Liquidation inventory: RPC-based (getTokenAccountsByOwner).
        // Liquidation is a manual safety action — RPC calls are acceptable here.
        // This bypasses JetStream snapshot staleness and ATA derivation issues.
        let token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
            .expect("valid token program");
        let token_2022_program_id = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
            .expect("valid token-2022 program");

        info!(wallet = %owner, "Liquidation: fetching wallet inventory via RPC (getTokenAccountsByOwner)");

        let mut rpc_token_accounts = ctx
            .rpc
            .rpc
            .get_token_accounts_by_owner(&owner, TokenAccountsFilter::ProgramId(token_program_id))
            .await
            .unwrap_or_default();

        if let Ok(mut accounts_2022) = ctx
            .rpc
            .rpc
            .get_token_accounts_by_owner(
                &owner,
                TokenAccountsFilter::ProgramId(token_2022_program_id),
            )
            .await
        {
            rpc_token_accounts.append(&mut accounts_2022);
        }

        info!(
            wallet = %owner,
            spl_plus_t22_accounts = rpc_token_accounts.len(),
            "Liquidation: RPC owner-scan complete"
        );

        // Parse RPC results into (mint, balance, decimals, token_program, token_account_pubkey)
        let mut inventory: Vec<(String, u64, u8, String, String)> = Vec::new();
        for ta in &rpc_token_accounts {
            let parsed = match &ta.account.data {
                UiAccountData::Json(parsed) => parsed,
                _ => continue,
            };
            let info = match parsed.parsed.get("info") {
                Some(v) => v,
                None => continue,
            };
            let mint_str = info
                .get("mint")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let balance_str = info
                .get("tokenAmount")
                .and_then(|v| v.get("amount"))
                .and_then(|v| v.as_str())
                .unwrap_or("0");
            let balance_raw: u64 = balance_str.parse().unwrap_or(0);
            let decimals = info
                .get("tokenAmount")
                .and_then(|v| v.get("decimals"))
                .and_then(|v| v.as_u64())
                .unwrap_or(6) as u8;
            let token_prog_str = ta.account.owner.clone();
            let ta_pubkey_str = ta.pubkey.clone();

            if balance_raw > 0 && mint_str != SOL_MINT && mint_str != WSOL_MINT {
                inventory.push((
                    mint_str,
                    balance_raw,
                    decimals,
                    token_prog_str,
                    ta_pubkey_str,
                ));
            }
        }

        info!(
            non_zero_positions = inventory.len(),
            "Liquidation: inventory filtered (non-zero, non-SOL/WSOL)"
        );

        // PA-4: filter inventory and cap seed amounts via PositionAuthority (no ghost LockManager seeds).
        let inventory = {
            let pa = ctx.position_authority.lock();
            let mut authority_filtered_inventory = Vec::with_capacity(inventory.len());
            for (mint_str, balance_raw, decimals, token_prog, ta_pubkey) in inventory {
                match liquidation_lockmanager_seed_decision(&pa, &mint_str, balance_raw) {
                    LiquidationSeedDecision::Skip => {
                        info!(
                            mint = %mint_str,
                            rpc_balance_raw = balance_raw,
                            "Liquidation: skipped LockManager seed (authority_closed_or_zero)"
                        );
                        record_liquidation_seed_skipped_authority_total();
                    }
                    LiquidationSeedDecision::Seed(seed_amount) if seed_amount > 0 => {
                        if seed_amount < balance_raw {
                            info!(
                                mint = %mint_str,
                                rpc_balance_raw = balance_raw,
                                authority_capped_seed = seed_amount,
                                "Liquidation: capped LockManager seed to PositionAuthority tradable"
                            );
                        }
                        authority_filtered_inventory.push((
                            mint_str,
                            seed_amount,
                            decimals,
                            token_prog,
                            ta_pubkey,
                        ));
                    }
                    LiquidationSeedDecision::Seed(_) => {}
                }
            }
            authority_filtered_inventory
        };

        info!(
            positions_after_authority_guard = inventory.len(),
            "Liquidation: inventory after PositionAuthority seed guard"
        );

        // Seed LockManager with RPC-discovered balances so that the
        // SIM_INSUFFICIENT_BALANCE preflight check passes for SELL intents.
        for (mint_str, balance_raw, _decimals, _token_prog, _ta_pubkey) in &inventory {
            ctx.lock_manager
                .set_available_token_balance(mint_str.clone(), *balance_raw);
            info!(
                mint = %mint_str,
                balance_raw = balance_raw,
                "Liquidation: seeded LockManager with RPC balance"
            );
        }

        for (mint_str, balance_raw, decimals, token_program_str, ta_pubkey_str) in &inventory {
            let balance_raw = *balance_raw;
            let decimals = *decimals;
            #[cfg(unix)]
            maybe_ping_watchdog();

            let mint = match Pubkey::from_str(mint_str) {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        mint = %mint_str,
                        error = %e,
                        balance_raw,
                        "LIQUIDATION SKIP: invalid mint pubkey, cannot parse"
                    );
                    continue;
                }
            };
            if mint == sol_mint {
                continue;
            }
            let token_program = match Pubkey::from_str(token_program_str) {
                Ok(p) => p,
                Err(_) => {
                    Self::token_program_for_mint_cached(ctx.live_pool_cache.as_deref(), &mint, None)
                }
            };
            #[cfg(unix)]
            maybe_ping_watchdog();
            // Use the actual token account pubkey from RPC (not derived ATA)
            let ta_pubkey = match Pubkey::from_str(ta_pubkey_str) {
                Ok(p) => p,
                Err(_) => Self::ata_for_owner_mint(&owner, &mint, &token_program),
            };
            let amount_in: u64 = balance_raw;
            #[cfg(unix)]
            maybe_ping_watchdog();

            // Build metadata/resources similar to sell-all.
            let mut metadata: HashMap<String, String> = HashMap::new();
            metadata.insert("purpose".to_string(), "liquidation".to_string());
            metadata.insert("kill_switch".to_string(), "true".to_string());
            metadata.insert("mint_decimals".to_string(), decimals.to_string());
            metadata.insert("token_account".to_string(), ta_pubkey.to_string());
            metadata.insert("token_program".to_string(), token_program.to_string());
            // Exit type/reason for Grafana dashboard display
            metadata.insert("exit_type".to_string(), "LIQUIDATION".to_string());
            if let Some(r) = &reason {
                metadata.insert("kill_reason".to_string(), r.clone());
                metadata.insert("exit_reason".to_string(), format!("Kill switch: {}", r));
            } else {
                metadata.insert(
                    "exit_reason".to_string(),
                    "Kill switch liquidation".to_string(),
                );
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

            // LIQUIDATION ROUTING (Cold Path, Scope 51):
            // - Known **active** PumpFun bonding curve (`LivePoolCache` complete=false): try direct
            //   PumpFun SELL first to avoid 45s PumpSwap quote/discovery before first SELL.
            // - Known **migrated** (complete) or Geyser-unknown: multi-pool + cache first; PumpFun
            //   only as last resort (stale quotes can 6005 on completed curves).
            // - 6005 at send time still triggers `try_6005_pumpfun_retry` (PumpSwap).

            let try_pumpfun_first =
                liquidation_pumpfun_sell_preference(ctx.live_pool_cache.as_deref(), &mint);

            if try_pumpfun_first {
                if let Some(ref pfun) = pumpfun {
                    min_out_sol = ctx
                        .liquidation_build_pumpfun_sell(
                            pfun,
                            &mint,
                            &sol_mint,
                            amount_in,
                            max_slippage_bps,
                            "pumpfun_preferred",
                            &mut metadata,
                            &mut resources,
                            &mut quote_attempts,
                        )
                        .await;
                    #[cfg(unix)]
                    maybe_ping_watchdog();
                }
            }

            if min_out_sol.is_none() {
                // I-24d Scope 21: one EnsureMeteoraDlmmPoolState + bounded JetStream wait per liquidation mint
                // when SLAVE has DLMM rows for this mint but none are explicitly Ready (no connector RPC).
                if let Some(ref cache) = ctx.live_pool_cache {
                    let rows = cache.meteora_dlmm_pools_for_mint(&mint);
                    if !rows.is_empty()
                        && !cache.base_mint_has_explicit_meteora_dlmm_ready_pool(&mint)
                    {
                        let pool_hint = rows
                            .iter()
                            .find(|(_, s)| s.token_x_mint == mint || s.token_y_mint == mint)
                            .map(|(pk, _)| pk.to_string());
                        warn!(
                            mint = %mint,
                            dlmm_rows = rows.len(),
                            pool_address_hint = ?pool_hint,
                            reason = "liquidation: Meteora DLMM pools in SLAVE but no explicit Ready — requesting EnsureMeteoraDlmmPoolState from market-data (bounded wait, one attempt per mint)",
                            "Meteora DLMM cold-path: pre-quote recovery request"
                        );
                        // Bug #34: capture evidence **before** the request (same as Orca cold-path). If we
                        // snapshot after `ControlResponse::Ok`, a fast JetStream merge can make `before`
                        // equal the post-recovery tuple and the wait never observes a change.
                        let hint_pk = pool_hint.as_deref().and_then(|s| Pubkey::from_str(s).ok());
                        let before_evidence = hint_pk.and_then(|pk| {
                            cache.get(&pk).and_then(|st| match st {
                                CachedPoolState::Meteora(s) => Some((
                                    s.active_id,
                                    s.bin_step,
                                    s.reserve_x_balance,
                                    s.reserve_y_balance,
                                )),
                                _ => None,
                            })
                        });
                        if let DiscoveryRequestOutcome::Ok = ctx
                            .request_meteora_dlmm_recovery_and_wait(
                                mint_str.as_str(),
                                pool_hint.as_deref(),
                            )
                            .await
                        {
                            if let Some(pk) = hint_pk {
                                if wait_for_meteora_dlmm_slave_after_recovery(
                                    cache,
                                    &pk,
                                    before_evidence,
                                    DISCOVERY_CACHE_WAIT_TIMEOUT_MS,
                                    DISCOVERY_CACHE_POLL_INTERVAL_MS,
                                )
                                .await
                                {
                                    for (pool_addr, ms) in cache.meteora_dlmm_pools_for_mint(&mint)
                                    {
                                        let _ =
                                            meteora.inject_cached_meteora_state(&pool_addr, &ms);
                                    }
                                    info!(
                                        mint = %mint,
                                        pool = %pk,
                                        "Meteora DLMM cold-path: SLAVE shows fresh explicit Ready after recovery — continuing liquidation quotes"
                                    );
                                } else {
                                    warn!(
                                        mint = %mint,
                                        pool = %pk,
                                        timeout_ms = DISCOVERY_CACHE_WAIT_TIMEOUT_MS,
                                        "Meteora DLMM cold-path: ControlResponse ok but bounded wait for SLAVE explicit Ready + fresh evidence timed out"
                                    );
                                }
                            }
                        }
                    }
                }

                // I-24d Scope 22: one EnsureRaydiumAmmPoolState + bounded JetStream wait per liquidation mint
                // when SLAVE has Raydium AMM rows for this mint but none are explicitly Ready (no connector RPC).
                if let Some(ref cache) = ctx.live_pool_cache {
                    let rows = cache.raydium_amm_pools_for_mint(&mint);
                    if !rows.is_empty()
                        && !cache.base_mint_has_explicit_raydium_amm_ready_pool(&mint)
                    {
                        let pool_hint = rows
                            .iter()
                            .find(|(_, s)| s.base_mint == mint || s.quote_mint == mint)
                            .map(|(pk, _)| pk.to_string());
                        warn!(
                            mint = %mint,
                            raydium_amm_rows = rows.len(),
                            pool_address_hint = ?pool_hint,
                            reason = "liquidation: Raydium AMM pools in SLAVE but no explicit Ready — requesting EnsureRaydiumAmmPoolState from market-data (bounded wait, one attempt per mint)",
                            "Raydium AMM cold-path: pre-quote recovery request"
                        );
                        let hint_pk = pool_hint.as_deref().and_then(|s| Pubkey::from_str(s).ok());
                        let before_evidence = hint_pk.and_then(|pk| {
                            cache
                                .raydium_amm_slave_readiness_snapshot(&pk)
                                .map(|(s, _)| {
                                    (
                                        s.coin_reserve.unwrap_or(0),
                                        s.pc_reserve.unwrap_or(0),
                                        s.serum_bids,
                                        s.serum_asks,
                                        s.serum_event_queue,
                                    )
                                })
                        });
                        if let DiscoveryRequestOutcome::Ok = ctx
                            .request_raydium_amm_recovery_and_wait(
                                mint_str.as_str(),
                                pool_hint.as_deref(),
                            )
                            .await
                        {
                            if let Some(pk) = hint_pk {
                                if wait_for_raydium_amm_slave_after_recovery(
                                    cache,
                                    &pk,
                                    before_evidence,
                                    DISCOVERY_CACHE_WAIT_TIMEOUT_MS,
                                    DISCOVERY_CACHE_POLL_INTERVAL_MS,
                                )
                                .await
                                {
                                    for (pool_addr, st) in cache.raydium_amm_pools_for_mint(&mint) {
                                        raydium.inject_cached_amm_state(
                                            pool_addr,
                                            st.base_mint,
                                            st.quote_mint,
                                            st.coin_vault,
                                            st.pc_vault,
                                            st.base_decimals,
                                            st.quote_decimals,
                                            st.coin_reserve,
                                            st.pc_reserve,
                                            st.market_id,
                                            st.serum_bids,
                                            st.serum_asks,
                                            st.serum_event_queue,
                                        );
                                    }
                                    info!(
                                        mint = %mint,
                                        pool = %pk,
                                        "Raydium AMM cold-path: SLAVE shows fresh explicit Ready after recovery — continuing liquidation quotes"
                                    );
                                } else {
                                    warn!(
                                        mint = %mint,
                                        pool = %pk,
                                        timeout_ms = DISCOVERY_CACHE_WAIT_TIMEOUT_MS,
                                        "Raydium AMM cold-path: ControlResponse ok but bounded wait for SLAVE explicit Ready + fresh evidence timed out"
                                    );
                                }
                            }
                        }
                    }
                }

                // --- Phase 1: Multi-pool routing (preferred for liquidation) ---
                {
                    let mut candidates: Vec<RouteCandidate> = Vec::new();
                    let mut record_candidate =
                        |dex: &str, amount_out: u64, pool_id: String, accounts: Vec<String>| {
                            candidates.push(RouteCandidate {
                                dex: dex.to_string(),
                                amount_out,
                                pool_id,
                                accounts,
                                creator: None,
                                execution_min_out_lamports: None,
                            });
                        };

                    // PumpSwap (Pump.fun AMM) with timeout guard.
                    // For LIQUIDATION: always try PumpSwap AMM regardless of bonding_curve_known_complete.
                    // The LivePoolCache may not know the curve is complete, but PumpSwap AMM
                    // might still have a pool. The RPC-based discovery in quote_exact_in handles this.
                    let pump_amm_quote = Some(
                        tokio::time::timeout(
                            Duration::from_secs(PUMPSWAP_LIQUIDATION_QUOTE_TIMEOUT_SECS),
                            pump_amm.quote_exact_in(
                                &mint.to_string(),
                                &sol_mint.to_string(),
                                amount_in,
                            ),
                        )
                        .await,
                    );
                    match pump_amm_quote {
                        None => {} // Already logged above
                        Some(Err(_timeout)) => {
                            quote_attempts.push(pump_amm_liquidation_quote_timeout_str());
                        }
                        Some(Ok(inner)) => match inner {
                            Ok(Some(q)) => {
                                #[cfg(unix)]
                                maybe_ping_watchdog();
                                if let Some(pool_id) = q.route.first().cloned() {
                                    // I-24d: Cache-only — no pump_amm.pool_accounts_v1_for_base_mint.
                                    let accounts = ctx.live_pool_cache.as_ref().and_then(|c| {
                                        c.get_ready_pump_amm_pool_accounts_by_base_mint(&mint)
                                    });
                                    match accounts {
                                        Some(accounts) if accounts.len() >= 14 => {
                                            let acct_strings: Vec<String> = accounts
                                                .into_iter()
                                                .map(|p| p.to_string())
                                                .collect();
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
                                        _ => {
                                            // I-24d: Request discovery, wait bounded on authoritative cache state.
                                            // pool_id from quote enables fast getAccount path in market-data.
                                            info!(mint = %mint, pool = %pool_id, "pump_amm quote ok but ready pool_accounts missing; requesting discovery (force_refresh)");
                                            match ctx
                                                .request_discovery_and_wait(
                                                    &mint.to_string(),
                                                    Some(pool_id.as_str()),
                                                    pump_amm_liquidation_discovery_force_refresh(),
                                                )
                                                .await
                                            {
                                                DiscoveryRequestOutcome::Ok => {
                                                    if let Some(cache) =
                                                        ctx.live_pool_cache.as_ref()
                                                    {
                                                        if wait_for_usable_pump_amm_cache_state(
                                                            cache,
                                                            &mint,
                                                            PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                                                            DISCOVERY_CACHE_POLL_INTERVAL_MS,
                                                        )
                                                        .await
                                                        {
                                                            let retry_quote = pump_amm
                                                                .quote_exact_in(
                                                                    &mint.to_string(),
                                                                    &sol_mint.to_string(),
                                                                    amount_in,
                                                                )
                                                                .await;
                                                            match retry_quote {
                                                                Ok(Some(ref rq))
                                                                    if rq.amount_out > 0 =>
                                                                {
                                                                    let pool_id = rq
                                                                        .route
                                                                        .first()
                                                                        .cloned()
                                                                        .unwrap_or(pool_id);
                                                                    let accounts = Pubkey::from_str(&pool_id)
                                                                        .ok()
                                                                        .and_then(|pk| {
                                                                            cache.get_ready_pump_amm_pool_accounts_for_pool_market(&pk)
                                                                        });
                                                                    if let Some(accounts) = accounts {
                                                                        if accounts.len() >= 14 {
                                                                            let acct_strings: Vec<String> =
                                                                                accounts
                                                                                    .into_iter()
                                                                                    .map(|p| p.to_string())
                                                                                    .collect();
                                                                            quote_attempts.push(format!(
                                                                    "pump_amm=ok amount_out={} pool={} accounts_len={} (after discovery)",
                                                                    rq.amount_out,
                                                                    pool_id,
                                                                    acct_strings.len()
                                                                ));
                                                                            record_candidate(
                                                                                "pump_amm",
                                                                                rq.amount_out,
                                                                                pool_id,
                                                                                acct_strings,
                                                                            );
                                                                        } else {
                                                                            quote_attempts.push(format!(
                                                                    "pump_amm=skip no_pool_accounts amount_out={} pool={}",
                                                                    rq.amount_out, pool_id
                                                                ));
                                                                        }
                                                                    } else {
                                                                        warn!(mint = %mint, "pump_amm pool_accounts still missing after discovery");
                                                                        quote_attempts.push(format!(
                                                                "pump_amm=skip no_pool_accounts amount_out={} pool={}",
                                                                rq.amount_out, pool_id
                                                            ));
                                                                    }
                                                                }
                                                                _ => {
                                                                    quote_attempts.push(format!(
                                                                    "pump_amm=skip zero_quote_after_discovery pool={}",
                                                                    pool_id
                                                                ));
                                                                }
                                                            }
                                                        } else {
                                                            log_pump_amm_slave_wait_timeout_evidence(
                                                                &mint,
                                                                &pool_id,
                                                                cache,
                                                                PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                                                            );
                                                            quote_attempts.push(format!(
                                                            "pump_amm=skip timeout amount_out={} pool={}",
                                                            q.amount_out, pool_id
                                                        ));
                                                        }
                                                    } else {
                                                        warn!(mint = %mint, "pump_amm: no cache");
                                                        quote_attempts.push(format!(
                                                        "pump_amm=skip no_cache amount_out={} pool={}",
                                                        q.amount_out, pool_id
                                                    ));
                                                    }
                                                }
                                                DiscoveryRequestOutcome::NotFound => {
                                                    warn!(mint = %mint, "pump_amm discovery not_found");
                                                    quote_attempts.push(format!(
                                                    "pump_amm=skip not_found amount_out={} pool={}",
                                                    q.amount_out, pool_id
                                                ));
                                                }
                                                DiscoveryRequestOutcome::Error(e) => {
                                                    warn!(mint = %mint, error = %e, "pump_amm discovery error");
                                                    quote_attempts.push(format!(
                                                        "pump_amm=err_discovery {e}"
                                                    ));
                                                }
                                                DiscoveryRequestOutcome::Timeout => {
                                                    warn!(mint = %mint, "pump_amm discovery timeout");
                                                    quote_attempts.push(format!(
                                                    "pump_amm=skip timeout amount_out={} pool={}",
                                                    q.amount_out, pool_id
                                                ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                // I-24d: Cold-Path Recovery — quote cache miss/degenerate, trigger Discovery-Request.
                                info!(
                                    mint = %mint,
                                    "pump_amm quote cache miss; requesting discovery from market-data"
                                );
                                let pool_hint = ctx
                                    .live_pool_cache
                                    .as_ref()
                                    .and_then(|c| c.get_pump_amm_pool_address_by_base_mint(&mint))
                                    .map(|p| p.to_string());
                                match ctx
                                    .request_discovery_and_wait(
                                        &mint.to_string(),
                                        pool_hint.as_deref(),
                                        pump_amm_liquidation_discovery_force_refresh(),
                                    )
                                    .await
                                {
                                    DiscoveryRequestOutcome::Ok => {
                                        if let Some(cache) = ctx.live_pool_cache.as_ref() {
                                            if wait_for_usable_pump_amm_cache_state(
                                                cache,
                                                &mint,
                                                PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                                                DISCOVERY_CACHE_POLL_INTERVAL_MS,
                                            )
                                            .await
                                            {
                                                // Retry quote_exact_in (cache may now have reserves + pool_accounts).
                                                let retry_quote = pump_amm
                                                    .quote_exact_in(
                                                        &mint.to_string(),
                                                        &sol_mint.to_string(),
                                                        amount_in,
                                                    )
                                                    .await;
                                                match retry_quote {
                                                    Ok(Some(q)) if q.amount_out > 0 => {
                                                        if let Some(pool_id) =
                                                            q.route.first().cloned()
                                                        {
                                                            let accounts = cache
                                                            .get_ready_pump_amm_pool_accounts_by_base_mint(&mint);
                                                            if let Some(accounts) = accounts {
                                                                if accounts.len() >= 14 {
                                                                    let acct_strings: Vec<String> =
                                                                        accounts
                                                                            .into_iter()
                                                                            .map(|p| p.to_string())
                                                                            .collect();
                                                                    quote_attempts.push(format!(
                                                                    "pump_amm=ok amount_out={} pool={} accounts_len={} (after discovery)",
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
                                                                } else {
                                                                    quote_attempts.push(format!(
                                                                    "pump_amm=skip no_pool_accounts amount_out={} pool={}",
                                                                    q.amount_out, pool_id
                                                                ));
                                                                }
                                                            } else {
                                                                quote_attempts.push(format!(
                                                                "pump_amm=skip no_pool_accounts amount_out={} pool={}",
                                                                q.amount_out, pool_id
                                                            ));
                                                            }
                                                        } else {
                                                            quote_attempts.push(
                                                                "pump_amm=none (after discovery)"
                                                                    .to_string(),
                                                            );
                                                        }
                                                    }
                                                    _ => {
                                                        quote_attempts.push(
                                                        "pump_amm=none (after discovery, zero amount_out or degenerate reserves)"
                                                            .to_string(),
                                                    );
                                                    }
                                                }
                                            } else {
                                                if let Some(pool_id) = pool_hint.as_deref() {
                                                    log_pump_amm_slave_wait_timeout_evidence(
                                                        &mint,
                                                        pool_id,
                                                        cache,
                                                        PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                                                    );
                                                } else {
                                                    warn!(
                                                        mint = %mint,
                                                        timeout_ms = PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                                                        "pump_amm: usable cache state not visible after discovery timeout (no pool hint)"
                                                    );
                                                }
                                                quote_attempts.push(
                                                "pump_amm=skip timeout (cache wait after discovery)"
                                                    .to_string(),
                                            );
                                            }
                                        } else {
                                            quote_attempts
                                                .push("pump_amm=skip no_cache".to_string());
                                        }
                                    }
                                    DiscoveryRequestOutcome::NotFound => {
                                        quote_attempts.push(
                                            "pump_amm=none (discovery not_found)".to_string(),
                                        );
                                    }
                                    DiscoveryRequestOutcome::Error(e) => {
                                        quote_attempts.push(format!("pump_amm=err_discovery {e}"));
                                    }
                                    DiscoveryRequestOutcome::Timeout => {
                                        quote_attempts
                                            .push("pump_amm=none (discovery timeout)".to_string());
                                    }
                                }
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
                                    let explicit_ready = ctx
                                        .live_pool_cache
                                        .as_ref()
                                        .map(|c| c.meteora_dlmm_pool_explicitly_ready(&pool_pk))
                                        .unwrap_or(false);
                                    if let Some(pool_accounts) = meteora.get_pool_accounts(&pool_pk)
                                    {
                                        let active_id = pool_accounts
                                            .get(5)
                                            .and_then(|s| s.strip_prefix("active_id:"))
                                            .and_then(|v| v.parse::<i32>().ok())
                                            .unwrap_or(0);

                                        if explicit_ready {
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
                                            "meteora=skip no_explicit_ready pool={} active_id={}",
                                            pool_id, active_id
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

                    let buildable = liquidation_filter_multi_pool_buildable_candidates(
                        ctx.live_pool_cache.as_deref(),
                        &orca,
                        &mint,
                        &sol_mint,
                        candidates,
                        &mut quote_attempts,
                    )
                    .await;
                    if let Some(best_route) = buildable.first().cloned() {
                        liquidation_store_multi_pool_fallback_metadata(&mut metadata, &buildable);
                        metadata.insert("sell_routing".to_string(), "multi_pool".to_string());
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
                                    s.token_mint == mint
                                        && s.creator != Pubkey::default()
                                        && !s.complete // Skip completed bonding curves (migrated to PumpSwap AMM)
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
                                    quote_attempts
                                        .push(format!("cache=skip no_quote pool={}", pool_addr));
                                    continue;
                                }
                                Err(e) => {
                                    quote_attempts
                                        .push(format!("cache=err pool={} err={}", pool_addr, e));
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
                                    if !cache.meteora_dlmm_pool_explicitly_ready(&pool_pk) {
                                        quote_attempts.push(format!(
                                            "cache=skip meteora no_explicit_ready pool={}",
                                            pool_id
                                        ));
                                        continue;
                                    }
                                    let Some(pool_accounts) = meteora.get_pool_accounts(&pool_pk)
                                    else {
                                        quote_attempts.push(format!(
                                            "cache=skip meteora no_pool_accounts pool={}",
                                            pool_id
                                        ));
                                        continue;
                                    };
                                    pool_accounts
                                }
                                CachedPoolState::Orca(_) => {
                                    let pool_pk = match Pubkey::from_str(&pool_id) {
                                        Ok(pk) => pk,
                                        Err(_) => continue,
                                    };
                                    let Some(pool_accounts) = orca.get_pool_accounts(&pool_pk)
                                    else {
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
                                execution_min_out_lamports: Some(min_out),
                            });
                        }

                        let buildable = liquidation_filter_multi_pool_buildable_candidates(
                            ctx.live_pool_cache.as_deref(),
                            &orca,
                            &mint,
                            &sol_mint,
                            candidates,
                            &mut quote_attempts,
                        )
                        .await;
                        if let Some(best_route) = buildable.first().cloned() {
                            liquidation_store_multi_pool_fallback_metadata(
                                &mut metadata,
                                &buildable,
                            );
                            metadata.insert("sell_routing".to_string(), "multi_pool".to_string());
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

                // Last resort: PumpFun bonding-curve SELL when multi-pool + cache had no route.
                // (Skip if we already tried pumpfun_preferred at the start — no duplicate work.)
                if min_out_sol.is_none() && !try_pumpfun_first {
                    if let Some(ref pfun) = pumpfun {
                        min_out_sol = ctx
                            .liquidation_build_pumpfun_sell(
                                pfun,
                                &mint,
                                &sol_mint,
                                amount_in,
                                max_slippage_bps,
                                "pumpfun_fallback",
                                &mut metadata,
                                &mut resources,
                                &mut quote_attempts,
                            )
                            .await;
                    }
                }
            } // end if min_out_sol.is_none() (Meteora/Raydium recovery + multi-pool + cache; last-resort pumpfun_fallback inside)

            let Some(min_out) = min_out_sol else {
                warn!(
                    mint = %mint,
                    amount_in,
                    token_account = %ta_pubkey,
                    quote_attempts = %quote_attempts.join(" | "),
                    "LIQUIDATION SKIP: No supported route found for token"
                );

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
            // Log BEFORE move so we can reference metadata/resources
            info!(
                mint = %mint,
                dex = metadata.get("dex").map(|s| s.as_str()).unwrap_or("?"),
                routing = metadata.get("sell_routing").map(|s| s.as_str()).unwrap_or("?"),
                creator_present = metadata.contains_key("creator"),
                amount_in,
                min_out,
                pools = ?resources.pools,
                quote_attempts = %quote_attempts.join(" | "),
                "LIQUIDATION: Intent prepared for token"
            );

            intent.ttl_ms = Some(ttl_ms);
            intent.resources = resources;
            intent.execution = Some(TradeExecutionConstraints {
                min_out: Some(ExplicitAmount::new(min_out, 9)),
            });
            intent.metadata.extend(metadata);

            // Stream: process each fully-prepared SELL as soon as it is ready (Scope 51). Discovery
            // for later inventory rows does not block earlier sends; still one sequential loop (no
            // parallel getProgramAccounts storms).
            #[cfg(unix)]
            maybe_ping_watchdog();

            match process_intent(&ctx, intent.clone()).await {
                Ok(()) => {}
                Err(e) => {
                    let mint_str = intent.resources.input_mint.clone();
                    let is_6005 = is_6005_bonding_curve_complete(&e);
                    let dex_pumpfun =
                        intent.metadata.get("dex").map(|s| s.as_str()) == Some("pumpfun");

                    if is_6005 && dex_pumpfun {
                        info!(
                            mint = %mint_str,
                            "6005-retry: BondingCurveComplete detected; retrying with PumpSwap AMM"
                        );
                        if let Some(retry) = ctx
                            .try_6005_pumpfun_retry(&intent, &pump_amm, max_slippage_bps)
                            .await
                        {
                            if let Err(retry_e) = process_intent(&ctx, retry).await {
                                warn!(
                                    mint = %mint_str,
                                    error = %retry_e,
                                    "6005-retry: PumpSwap AMM attempt also failed"
                                );
                            }
                        }
                    } else {
                        warn!(error = %e, "Liquidation intent processing failed");
                    }
                }
            }
        }

        // === Retry Phase: Re-scan wallet for tokens still present ===
        // First pass may have failed for some tokens (stale quotes, RPC issues, sim failures).
        // Wait for first-pass TXs to confirm, then re-scan and retry failed tokens.
        info!("Liquidation first pass complete. Waiting before retry scan...");
        tokio::time::sleep(Duration::from_secs(10)).await;
        #[cfg(unix)]
        maybe_ping_watchdog();

        let token_program_id = Pubkey::new_from_array(spl_token::id().to_bytes());
        let token_2022_program_id = Pubkey::new_from_array(spl_token_2022::id().to_bytes());
        let mut retry_rpc_accounts = ctx
            .rpc
            .rpc
            .get_token_accounts_by_owner(&owner, TokenAccountsFilter::ProgramId(token_program_id))
            .await
            .unwrap_or_default();

        if let Ok(mut accounts_2022) = ctx
            .rpc
            .rpc
            .get_token_accounts_by_owner(
                &owner,
                TokenAccountsFilter::ProgramId(token_2022_program_id),
            )
            .await
        {
            retry_rpc_accounts.append(&mut accounts_2022);
        }

        let mut retry_count = 0u32;
        for ta in &retry_rpc_accounts {
            let parsed = match &ta.account.data {
                UiAccountData::Json(parsed) => parsed,
                _ => continue,
            };
            let info = match parsed.parsed.get("info") {
                Some(v) => v,
                None => continue,
            };
            let mint_str = info
                .get("mint")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let balance_str = info
                .get("tokenAmount")
                .and_then(|v| v.get("amount"))
                .and_then(|v| v.as_str())
                .unwrap_or("0");
            let balance_raw: u64 = balance_str.parse().unwrap_or(0);

            if balance_raw == 0 || mint_str == SOL_MINT || mint_str == WSOL_MINT {
                continue;
            }

            retry_count += 1;
            warn!(
                mint = %mint_str,
                balance_raw,
                "LIQUIDATION RETRY: Token still in wallet after first pass"
            );
        }

        if retry_count > 0 {
            warn!(
                remaining_tokens = retry_count,
                "Liquidation: {} tokens still in wallet — full retry would require re-routing (logged for diagnostics)",
                retry_count
            );
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
                                info!(wallet = %wallet, wsol_ata = %wsol_ata, signature = %sig, "Unwrapped WSOL (closed ATA)");
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
        let burn_lpc = ctx.live_pool_cache.as_ref().map(Arc::clone);
        let raydium = Raydium::new_with_live_cache(
            Arc::clone(&ctx.rpc),
            burn_lpc.clone(),
            false, // I-24d: Raydium AMM route check uses cache + market-data EnsureRaydiumAmmPoolState, not connector scan
        );
        let burn_pumpfun_cache = burn_lpc;
        let pumpfun = match PumpFunDex::new(Arc::clone(&ctx.rpc), burn_pumpfun_cache) {
            Ok(p) => Some(p),
            Err(e) => {
                warn!(error = %e, "Failed to init PumpFunDex in burn job; continuing with Raydium only");
                None
            }
        };
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
            let decimals = get_token_decimals_or_default(
                ctx.rpc.as_ref(),
                &mint,
                ctx.live_pool_cache.as_deref(),
            )
            .await;
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
                if let Some(ref cache) = ctx.live_pool_cache {
                    let rows = cache.raydium_amm_pools_for_mint(&mint);
                    if !rows.is_empty()
                        && !cache.base_mint_has_explicit_raydium_amm_ready_pool(&mint)
                    {
                        let pool_hint = rows
                            .iter()
                            .find(|(_, s)| s.base_mint == mint || s.quote_mint == mint)
                            .map(|(pk, _)| pk.to_string());
                        warn!(
                            request_id = %request_id,
                            mint = %mint,
                            raydium_amm_rows = rows.len(),
                            pool_address_hint = ?pool_hint,
                            reason = "manual burn: Raydium AMM in SLAVE but no explicit Ready — EnsureRaydiumAmmPoolState from market-data (bounded wait, one attempt)",
                            "Raydium AMM cold-path: burn job pre-quote recovery"
                        );
                        let hint_pk = pool_hint.as_deref().and_then(|s| Pubkey::from_str(s).ok());
                        let before_evidence = hint_pk.and_then(|pk| {
                            cache
                                .raydium_amm_slave_readiness_snapshot(&pk)
                                .map(|(s, _)| {
                                    (
                                        s.coin_reserve.unwrap_or(0),
                                        s.pc_reserve.unwrap_or(0),
                                        s.serum_bids,
                                        s.serum_asks,
                                        s.serum_event_queue,
                                    )
                                })
                        });
                        if let DiscoveryRequestOutcome::Ok = ctx
                            .request_raydium_amm_recovery_and_wait(
                                &mint.to_string(),
                                pool_hint.as_deref(),
                            )
                            .await
                        {
                            if let Some(pk) = hint_pk {
                                if wait_for_raydium_amm_slave_after_recovery(
                                    cache,
                                    &pk,
                                    before_evidence,
                                    DISCOVERY_CACHE_WAIT_TIMEOUT_MS,
                                    DISCOVERY_CACHE_POLL_INTERVAL_MS,
                                )
                                .await
                                {
                                    for (pool_addr, st) in cache.raydium_amm_pools_for_mint(&mint) {
                                        raydium.inject_cached_amm_state(
                                            pool_addr,
                                            st.base_mint,
                                            st.quote_mint,
                                            st.coin_vault,
                                            st.pc_vault,
                                            st.base_decimals,
                                            st.quote_decimals,
                                            st.coin_reserve,
                                            st.pc_reserve,
                                            st.market_id,
                                            st.serum_bids,
                                            st.serum_asks,
                                            st.serum_event_queue,
                                        );
                                    }
                                    info!(
                                        request_id = %request_id,
                                        mint = %mint,
                                        pool = %pk,
                                        "Raydium AMM cold-path: burn job SLAVE Ready after recovery"
                                    );
                                } else {
                                    warn!(
                                        request_id = %request_id,
                                        mint = %mint,
                                        pool = %pk,
                                        timeout_ms = DISCOVERY_CACHE_WAIT_TIMEOUT_MS,
                                        "Raydium AMM cold-path: burn job bounded wait timed out after ControlResponse ok"
                                    );
                                }
                            }
                        }
                    }
                }
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
                "intent_ttl_ms" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.intent_ttl_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "capital_lock_ttl_buffer_ms" => {
                    if let Some(v) = value.as_u64() {
                        if (1_000..=120_000).contains(&v) {
                            config.capital_lock_ttl_buffer_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1000-120000".to_string()));
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
                "jetstream_tx_confirm_enabled" | "geyser_confirm_enabled" => {
                    if let Some(v) = value.as_bool() {
                        config.jetstream_tx_confirm_enabled = v;
                        applied.push(key.clone());
                        if key == "geyser_confirm_enabled" {
                            info!(
                                key = %key,
                                new_value = %v,
                                "Config updated (deprecated key; use jetstream_tx_confirm_enabled)"
                            );
                        } else {
                            info!(key = %key, new_value = %v, "Config updated");
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "confirm_commitment" => {
                    if let Some(v) = value.as_str() {
                        let v_lc = v.to_lowercase();
                        match v_lc.as_str() {
                            "finalized" | "confirmed" => {
                                config.confirm_commitment = v_lc;
                                applied.push(key.clone());
                                info!(
                                    key = %key,
                                    new_value = %v,
                                    "Config updated (EE confirm waits on JetStream; commitment is market-data Geyser subscription at startup)"
                                );
                            }
                            _ => rejected.push((
                                key.clone(),
                                "Must be one of: finalized, confirmed".to_string(),
                            )),
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected string".to_string()));
                    }
                }
                "rebroadcast_interval_ms" => {
                    if let Some(v) = value.as_u64() {
                        if (500..=30_000).contains(&v) {
                            config.rebroadcast_interval_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 500-30000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "max_rebroadcasts" => {
                    if let Some(v) = value.as_u64() {
                        if (0..=20).contains(&v) {
                            config.max_rebroadcasts = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 0-20".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "rebroadcast_use_tpu" => {
                    if let Some(v) = value.as_bool() {
                        config.rebroadcast_use_tpu = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
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
                // === FIX-31: Parallel Intent Processing ===
                "max_concurrent_intents" => {
                    if let Some(v) = value.as_u64() {
                        if (1..=16).contains(&v) {
                            info!(key = %key, new_value = %v, "Config acknowledged (restart required to take effect)");
                            applied.push(key.clone());
                        } else {
                            rejected.push((key.clone(), "Out of range (1-16)".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
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

    /// LockManager non-zero token balances (state snapshot persistence; not primary metrics / BUY gate).
    fn get_open_positions(&self) -> usize {
        self.lock_manager.count_non_zero_token_balances()
    }

    /// PA-3: PositionAuthority open count for max_open_positions BUY gate (in-process, no RPC).
    fn get_position_authority_open_positions_count(&self) -> Option<usize> {
        Some(self.position_authority.lock().open_positions_count())
    }

    fn apply_position_authority_from_execution_result(&self, exec: &ExecutionResult) {
        {
            let mut pa = self.position_authority.lock();
            let changes = pa.apply_from_confirmed_execution_result(exec);
            self.enqueue_position_authority_kv_publish(&changes);
        }
        self.refresh_position_authority_metrics();
    }

    /// Scope C: EE-only / authority BUY confirms publish Position pin when routed pool is known.
    async fn publish_open_position_pool_pin_after_confirmed_buy(
        &self,
        intent: &TradeIntent,
        exec: &ExecutionResult,
    ) {
        if exec.status != ExecutionStatus::Confirmed
            || !should_publish_open_position_pool_pin_after_confirmed_buy(intent)
        {
            return;
        }
        let pool = intent.resources.pools.first().expect("checked in filter");
        let mint = intent.resources.output_mint.as_str();
        let Some(nats) = self.nats.as_ref() else {
            return;
        };
        let update = MomentumActivePoolsUpdate {
            version: MOMENTUM_ACTIVE_POOLS_WIRE_VERSION,
            ts_unix_ms: wall_clock_unix_ms_now(),
            active: vec![MomentumActivePoolEntry {
                mint: mint.to_string(),
                pool: pool.clone(),
                pin_reason: MomentumActivePinReason::Position,
            }],
            removed: vec![],
            full_active_snapshot: false,
        };
        if let Err(e) = nats.publish(TOPIC_MOMENTUM_ACTIVE_POOLS, &update).await {
            warn!(
                error = %e,
                intent_id = %intent.intent_id,
                mint = %mint,
                pool = %pool,
                "Failed to publish open-position pool pin for confirmed BUY"
            );
        }
    }

    /// Wallet snapshot: same filter as `PositionEvent::try_from_market_event_kind` (ignores SOL/WSOL for authority open count).
    fn apply_position_authority_from_wallet_event_kind(&self, kind: &MarketEventKind) {
        {
            let mut pa = self.position_authority.lock();
            let changes = pa.apply_from_wallet_market_event_kind(kind);
            self.enqueue_position_authority_kv_publish(&changes);
        }
        self.refresh_position_authority_metrics();
    }

    /// PA-5.1 / PA-6b: enqueue KV publish while holding reducer lock (preserves apply order).
    fn enqueue_position_authority_kv_publish(&self, changes: &[PositionAuthorityChange]) {
        self.position_authority_kv_publisher.enqueue(changes);
    }

    /// Apply a WalletBalanceSnapshot MarketEvent to LockManager + decimals cache.
    /// NATIVE_SOL / WSOL handlers stay separate (KNOWN_BUG #23).
    fn apply_wallet_balance_snapshot_event(&self, event: &MarketEvent) {
        if let MarketEventKind::WalletBalanceSnapshot {
            mint,
            balance_raw,
            decimals,
            ..
        } = &event.kind
        {
            if mint == "NATIVE_SOL" {
                self.lock_manager.update_native_sol_only(*balance_raw);
                if let Some(ref tx) = self.wsol_balance_tx {
                    let wsol = self.lock_manager.wsol_balance();
                    let _ = tx.try_send((*balance_raw, Some(wsol)));
                }
            } else if mint == WSOL_MINT || mint == SOL_MINT {
                let incoming = *balance_raw;
                let effective_wsol = if let Some(ref pending) = self.wsol_pending_wrap {
                    let (effective, confirms, was_floored) =
                        pending.effective_wsol_for_snapshot(incoming);
                    if was_floored {
                        ironcrab::metrics::WSOL_LOCK_MANAGER_SNAPSHOT_FLOORED_TOTAL
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(
                            event = "lock_manager_stale_wsol_snapshot_floored_due_to_pending_wrap",
                            incoming_wsol_lamports = incoming,
                            pending_expected_wsol_lamports = pending.pending_expected(),
                            effective_wsol_lamports = effective,
                            "Floored LockManager WSOL snapshot while pending post-wrap confirmation"
                        );
                    }
                    if confirms {
                        pending.clear();
                    }
                    effective
                } else {
                    incoming
                };
                self.lock_manager.update_wsol_only(effective_wsol);
                if let Some(ref tx) = self.wsol_balance_tx {
                    let sol = self.lock_manager.total_native_sol();
                    let _ = tx.try_send((sol, Some(incoming)));
                }
            } else {
                let old = self.lock_manager.available_token_balance(mint);
                self.lock_manager
                    .set_available_token_balance(mint.clone(), *balance_raw);

                if old != *balance_raw {
                    info!(
                        mint = %mint,
                        old_balance = old,
                        new_balance = *balance_raw,
                        "Token balance sync: LockManager updated from Geyser"
                    );
                }

                if let (Some(cache), Ok(mint_pk)) =
                    (self.live_pool_cache.as_ref(), Pubkey::from_str(mint))
                {
                    cache.set_mint_decimals(mint_pk, *decimals);
                }
            }
        }
        self.apply_position_authority_from_wallet_event_kind(&event.kind);
    }

    fn refresh_position_authority_metrics(&self) {
        let a = self.position_authority.lock();
        let auth_open = a.open_positions_count();
        let reconcile = a.reconcile_needed_positions_count();
        let lock_open = self.lock_manager.count_non_zero_token_balances();
        let drift = position_authority_drift_lockmanager(auth_open, lock_open);
        drop(a);
        // PA-2 Rest: primary `open_positions` gauge follows PositionAuthority (I-24a).
        OPEN_POSITIONS_GAUGE.store(auth_open as u64, Ordering::Relaxed);
        POSITION_AUTHORITY_OPEN_GAUGE.store(auth_open as u64, Ordering::Relaxed);
        POSITION_AUTHORITY_RECONCILE_NEEDED_GAUGE.store(reconcile as u64, Ordering::Relaxed);
        POSITION_AUTHORITY_LOCKMANAGER_OPEN_GAUGE.store(lock_open as u64, Ordering::Relaxed);
        POSITION_AUTHORITY_DRIFT_LOCKMANAGER.store(drift, Ordering::Relaxed);
    }
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

// build_minimal_pool_state and bootstrap_pool_cache_from_jetstream are now shared
// via ironcrab::execution::pool_cache_sync (used by both execution-engine and momentum-bot).
use ironcrab::execution::pool_cache_sync::{
    apply_pool_cache_update, bootstrap_pool_cache_from_jetstream,
};

/// Bootstrap open positions count from wallet snapshots (JetStream)
///
/// Uses last-per-subject WalletBalanceSnapshot events to count non-zero mints.
/// Returns None if no snapshots were found (do not override persisted state).
/// NOTE: open_positions is now derived from LockManager.count_non_zero_token_balances();
/// this function is kept for potential diagnostics/future use.
#[allow(dead_code)]
async fn bootstrap_open_positions_from_wallet_snapshot(
    nats_client: &NatsClient,
    wallet: &Pubkey,
) -> Result<Option<usize>> {
    use async_nats::jetstream;
    use futures::StreamExt;

    let jetstream = jetstream::new(nats_client.client().clone());
    let stream = match jetstream.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = WALLET_SNAPSHOT_STREAM_NAME,
                "Wallet snapshot stream not found (market-data may not be running)"
            );
            return Ok(None);
        }
    };

    let wallet_str = wallet.to_string();
    let mut consumer_config = wallet_snapshot_consumer_config();
    consumer_config.filter_subject = format!("ironcrab.wallet_snapshot.{}.*", wallet_str);

    let consumer = stream.create_consumer(consumer_config).await?;
    let mut mints: HashSet<String> = HashSet::new();
    let mut observed = 0usize;
    let batch_size = 1000;

    loop {
        let mut messages = consumer.fetch().max_messages(batch_size).messages().await?;
        let mut batch_count = 0;

        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "Error fetching wallet snapshot from JetStream");
                    continue;
                }
            };

            batch_count += 1;

            let event: MarketEvent = match serde_json::from_slice(&msg.payload) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "Failed to deserialize WalletBalanceSnapshot from JetStream");
                    if let Err(ack_err) = msg.ack().await {
                        warn!(error = %ack_err, "Failed to ack wallet snapshot message");
                    }
                    continue;
                }
            };

            if let MarketEventKind::WalletBalanceSnapshot {
                mint, balance_raw, ..
            } = &event.kind
            {
                observed += 1;
                // FIX-36: Skip SOL/WSOL — they're the quote currency, not tradeable positions.
                // Belt-and-suspenders: SOL_MINT == WSOL_MINT, but WSOL_MINT explicit for clarity.
                if mint == SOL_MINT
                    || mint == WSOL_MINT
                    || mint == "NATIVE_SOL"
                    || mint == "11111111111111111111111111111111"
                {
                    continue;
                }
                if *balance_raw > 0 {
                    mints.insert(mint.clone());
                }
            }

            if let Err(ack_err) = msg.ack().await {
                warn!(error = %ack_err, "Failed to ack wallet snapshot message");
            }
        }

        if batch_count < batch_size {
            break;
        }
    }

    if observed == 0 {
        info!(
            wallet = %wallet_str,
            "Wallet snapshot bootstrap: no snapshots found (keeping persisted open_positions)"
        );
        return Ok(None);
    }

    info!(
        wallet = %wallet_str,
        snapshots = observed,
        open_positions = mints.len(),
        "Wallet snapshot bootstrap: open_positions reconciled from wallet"
    );

    Ok(Some(mints.len()))
}

/// Bootstrap token balances AND wallet SOL/WSOL from wallet snapshots (JetStream).
///
/// Uses last-per-subject WalletBalanceSnapshot events to seed LockManager.available_tokens
/// AND LockManager wallet balances (SOL + WSOL) without any RPC calls.
/// This replaces the hardcoded 1 SOL default and eliminates the race condition where
/// market-data's initial WalletBalanceUpdate (core NATS, fire-and-forget) could be
/// missed if execution-engine hadn't subscribed yet.
/// Live updates continue via JetStream consumer in the main loop.
async fn bootstrap_token_balances_from_wallet_snapshot(
    nats_client: &NatsClient,
    wallet: &Pubkey,
    lock_manager: &LockManager,
) -> Result<(usize, Vec<MarketEventKind>)> {
    use async_nats::jetstream;
    use futures::StreamExt;

    let jetstream = jetstream::new(nats_client.client().clone());
    let stream = match jetstream.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = WALLET_SNAPSHOT_STREAM_NAME,
                "Wallet snapshot stream not found (market-data may not be running)"
            );
            return Ok((0, Vec::new()));
        }
    };

    let wallet_str = wallet.to_string();
    let mut consumer_config = wallet_snapshot_consumer_config();
    consumer_config.filter_subject = format!("ironcrab.wallet_snapshot.{}.*", wallet_str);

    let consumer = stream.create_consumer(consumer_config).await?;
    let batch_size = 1000;
    let mut observed = 0usize;
    let mut bootstrap_sol: Option<u64> = None;
    let mut bootstrap_wsol: Option<u64> = None;
    let mut wallet_snapshot_kinds = Vec::new();

    loop {
        let mut messages = consumer.fetch().max_messages(batch_size).messages().await?;
        let mut batch_count = 0;

        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    debug!(error = %e, "Error fetching wallet snapshot from JetStream");
                    continue;
                }
            };
            batch_count += 1;

            let event: MarketEvent = match serde_json::from_slice(&msg.payload) {
                Ok(e) => e,
                Err(e) => {
                    debug!(error = %e, "Failed to deserialize WalletBalanceSnapshot from JetStream");
                    let _ = msg.ack().await;
                    continue;
                }
            };

            if let MarketEventKind::WalletBalanceSnapshot {
                mint, balance_raw, ..
            } = &event.kind
            {
                observed += 1;
                if mint == "NATIVE_SOL" {
                    // Sentinel for native SOL (system account lamports)
                    bootstrap_sol = Some(*balance_raw);
                } else if mint == WSOL_MINT {
                    // WSOL SPL token balance (same mint address as SOL_MINT)
                    bootstrap_wsol = Some(*balance_raw);
                } else if mint != SOL_MINT {
                    // Regular token balance (skip SOL_MINT which equals WSOL_MINT
                    // but could appear from old JetStream entries)
                    lock_manager.set_available_token_balance(mint.clone(), *balance_raw);
                    wallet_snapshot_kinds.push(event.kind.clone());
                }
            } else if matches!(event.kind, MarketEventKind::WalletSnapshotComplete { .. }) {
                observed += 1;
                wallet_snapshot_kinds.push(event.kind.clone());
            }

            let _ = msg.ack().await;
        }

        if batch_count < batch_size {
            break;
        }
    }

    // Update LockManager with authoritative SOL/WSOL from JetStream
    if bootstrap_sol.is_some() || bootstrap_wsol.is_some() {
        let sol = bootstrap_sol.unwrap_or_else(|| lock_manager.total_native_sol());
        // If we have SOL but no WSOL in JetStream (e.g. market-data not run yet), assume 0
        // rather than leaving available_wsol uninitialized (which would show wrong metrics).
        let wsol = bootstrap_wsol.or(if bootstrap_sol.is_some() {
            Some(0)
        } else {
            None
        });
        lock_manager.update_wallet_balances(sol, wsol);
        info!(
            wallet = %wallet_str,
            sol_lamports = sol,
            wsol_lamports = ?wsol,
            sol = sol as f64 / 1e9,
            wsol = wsol.map(|w| w as f64 / 1e9),
            "Wallet snapshot bootstrap: SOL/WSOL balances seeded into LockManager from JetStream"
        );
    }

    if observed > 0 {
        info!(
            wallet = %wallet_str,
            snapshots = observed,
            "Wallet snapshot bootstrap: token balances seeded into LockManager"
        );
    }

    Ok((observed, wallet_snapshot_kinds))
}

/// Create the durable live wallet snapshot consumer for the main loop.
///
/// When `bootstrap_observed == 0` (stream missing at bootstrap or no snapshots), uses
/// `LastPerSubject` so the consumer backfills latest per-mint snapshots when the stream
/// appears later. Otherwise uses `New` to avoid replaying history already seeded by bootstrap.
async fn create_wallet_snapshot_live_consumer(
    nats_client: &NatsClient,
    wallet: &Pubkey,
    bootstrap_observed: usize,
) -> Option<async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>>
{
    use async_nats::jetstream;

    let jetstream = jetstream::new(nats_client.client().clone());
    let stream = match jetstream.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = WALLET_SNAPSHOT_STREAM_NAME,
                "JetStream wallet snapshot stream not found (market-data may not be running)"
            );
            return None;
        }
    };

    let wallet_str = wallet.to_string();
    let mut cfg = wallet_snapshot_live_consumer_config_execution_engine(&wallet_str);
    if bootstrap_observed == 0 {
        cfg.deliver_policy = jetstream::consumer::DeliverPolicy::LastPerSubject;
    }

    match stream.create_consumer(cfg).await {
        Ok(consumer) => {
            info!(
                stream = WALLET_SNAPSHOT_STREAM_NAME,
                wallet = %wallet_str,
                deliver_policy = if bootstrap_observed == 0 {
                    "LastPerSubject"
                } else {
                    "New"
                },
                "Subscribed to JetStream WalletBalanceSnapshot (live)"
            );
            Some(consumer)
        }
        Err(e) => {
            warn!(
                error = %e,
                "Failed to create JetStream consumer for WalletBalanceSnapshot"
            );
            None
        }
    }
}

/// Create the durable live WalletTxConfirmed consumer for the main loop (PR3).
async fn create_wallet_tx_confirm_live_consumer(
    nats_client: &NatsClient,
    wallet: &Pubkey,
) -> Option<async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>>
{
    use async_nats::jetstream;

    let jetstream = jetstream::new(nats_client.client().clone());
    let stream = match jetstream.get_stream(WALLET_TX_CONFIRM_STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = WALLET_TX_CONFIRM_STREAM_NAME,
                "JetStream wallet TX confirm stream not found (market-data may not be running)"
            );
            return None;
        }
    };

    let wallet_str = wallet.to_string();
    let cfg = wallet_tx_confirm_live_consumer_config_execution_engine(&wallet_str);

    match stream.create_consumer(cfg).await {
        Ok(consumer) => {
            info!(
                stream = WALLET_TX_CONFIRM_STREAM_NAME,
                wallet = %wallet_str,
                "Subscribed to JetStream WalletTxConfirmed (live)"
            );
            Some(consumer)
        }
        Err(e) => {
            warn!(
                error = %e,
                stream = WALLET_TX_CONFIRM_STREAM_NAME,
                "Failed to create JetStream consumer for WalletTxConfirmed"
            );
            None
        }
    }
}

const INTENT_CHANNEL_ENQUEUE_TRACKER_CAP: usize = 256;

/// Sidecar map: channel enqueue wall time keyed by `intent_id` (not on-wire; cap 256, LRU evict).
struct IntentChannelEnqueueTracker {
    inner: std::sync::Mutex<IntentChannelEnqueueState>,
}

struct IntentChannelEnqueueState {
    enqueue_ms_by_id: HashMap<String, u64>,
    insertion_order: VecDeque<String>,
}

impl Default for IntentChannelEnqueueTracker {
    fn default() -> Self {
        Self {
            inner: std::sync::Mutex::new(IntentChannelEnqueueState {
                enqueue_ms_by_id: HashMap::new(),
                insertion_order: VecDeque::new(),
            }),
        }
    }
}

impl IntentChannelEnqueueTracker {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record_enqueue(&self, intent_id: String, enqueue_ms: u64) {
        let mut state = self
            .inner
            .lock()
            .expect("IntentChannelEnqueueTracker lock poisoned");
        if let Some(entry) = state.enqueue_ms_by_id.get_mut(&intent_id) {
            *entry = enqueue_ms;
            return;
        }
        while state.enqueue_ms_by_id.len() >= INTENT_CHANNEL_ENQUEUE_TRACKER_CAP {
            if let Some(oldest) = state.insertion_order.pop_front() {
                state.enqueue_ms_by_id.remove(&oldest);
            } else {
                break;
            }
        }
        state.insertion_order.push_back(intent_id.clone());
        state.enqueue_ms_by_id.insert(intent_id, enqueue_ms);
    }

    fn take_and_record_channel_wait(&self, intent_id: &str, now_ms: u64) {
        let enqueue_ms = {
            let mut state = self
                .inner
                .lock()
                .expect("IntentChannelEnqueueTracker lock poisoned");
            let Some(enqueue_ms) = state.enqueue_ms_by_id.remove(intent_id) else {
                return;
            };
            if let Some(pos) = state.insertion_order.iter().position(|id| id == intent_id) {
                state.insertion_order.remove(pos);
            }
            enqueue_ms
        };
        record_execution_intent_channel_wait_ms(now_ms.saturating_sub(enqueue_ms));
    }
}

/// Log when pending intents in `intent_rx` exceed this depth (backpressure visibility).
const INTENT_RX_QUEUE_DEPTH_WARN_THRESHOLD: u64 = 10;

/// Max JetStream messages processed per cold-path consumer iteration (Pool/Wallet/TX-confirm).
const EE_JETSTREAM_CONSUMER_BATCH_MAX: usize = 15;
const EE_JETSTREAM_CONSUMER_FETCH_EXPIRES: Duration = Duration::from_millis(100);
/// Backoff when WalletTxConfirmed consumer is missing (matches former 100ms poll interval).
const EE_WALLET_TX_CONFIRM_RECREATE_BACKOFF: Duration = Duration::from_millis(100);
/// Warn when JetStream consumer `num_pending` exceeds this threshold.
const EE_CONSUMER_PENDING_WARN_THRESHOLD: u64 = 100;

type JetStreamPullConsumer =
    async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>;

async fn create_pool_cache_live_consumer_for_ee(
    nats_client: &NatsClient,
) -> Option<JetStreamPullConsumer> {
    use async_nats::jetstream;

    let jetstream = jetstream::new(nats_client.client().clone());
    let stream = match jetstream.get_stream(STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = STREAM_NAME,
                "JetStream stream not found for live PoolCacheUpdate sync (market-data may not be running)"
            );
            return None;
        }
    };

    match stream
        .create_consumer(pool_cache_live_consumer_config_execution_engine())
        .await
    {
        Ok(consumer) => {
            info!(
                stream = STREAM_NAME,
                deliver_policy = "New",
                "Subscribed to JetStream PoolCacheUpdate (live, cold-path task)"
            );
            Some(consumer)
        }
        Err(e) => {
            warn!(error = %e, "Failed to create JetStream consumer for live PoolCacheUpdate sync");
            None
        }
    }
}

async fn refresh_pool_cache_consumer_metrics(consumer: &mut JetStreamPullConsumer) {
    match consumer.info().await {
        Ok(info) => {
            set_execution_pool_cache_consumer_pending(info.num_pending);
            if info.num_pending > EE_CONSUMER_PENDING_WARN_THRESHOLD {
                warn!(
                    pending = info.num_pending,
                    threshold = EE_CONSUMER_PENDING_WARN_THRESHOLD,
                    "Pool cache JetStream consumer backlog above threshold"
                );
            }
        }
        Err(e) => {
            debug!(error = %e, "Failed to read pool cache consumer info for metrics");
        }
    }
}

async fn refresh_wallet_snapshot_consumer_metrics(consumer: &mut JetStreamPullConsumer) {
    match consumer.info().await {
        Ok(info) => {
            set_execution_wallet_snapshot_consumer_pending(info.num_pending);
            if info.num_pending > EE_CONSUMER_PENDING_WARN_THRESHOLD {
                warn!(
                    pending = info.num_pending,
                    threshold = EE_CONSUMER_PENDING_WARN_THRESHOLD,
                    "Wallet snapshot JetStream consumer backlog above threshold"
                );
            }
        }
        Err(e) => {
            debug!(error = %e, "Failed to read wallet snapshot consumer info for metrics");
        }
    }
}

/// Cold-path task: PoolCacheUpdate JetStream → LivePoolCache (bounded batches + yield).
async fn run_pool_cache_consumer_task(
    live_pool_cache: SharedLivePoolCache,
    consumer: JetStreamPullConsumer,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    use futures::StreamExt;

    let mut consumer = consumer;
    let mut info_poll_counter: u32 = 0;

    loop {
        if *shutdown_rx.borrow() {
            info!("Pool cache consumer task shutting down");
            break;
        }

        match consumer
            .fetch()
            .max_messages(EE_JETSTREAM_CONSUMER_BATCH_MAX)
            .expires(EE_JETSTREAM_CONSUMER_FETCH_EXPIRES)
            .messages()
            .await
        {
            Ok(mut messages) => {
                let mut processed = 0u64;
                while let Some(msg_result) = messages.next().await {
                    match msg_result {
                        Ok(msg) => {
                            match serde_json::from_slice::<PoolCacheUpdate>(&msg.payload) {
                                Ok(update) => {
                                    if apply_pool_cache_update(&live_pool_cache, &update) {
                                        processed += 1;
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "Failed to deserialize PoolCacheUpdate");
                                }
                            }
                            if let Err(e) = msg.ack().await {
                                warn!(error = %e, "Failed to ack JetStream PoolCacheUpdate message");
                            }
                        }
                        Err(e) => {
                            debug!(error = %e, "JetStream pool cache fetch returned error");
                        }
                    }
                }
                if processed > 0 {
                    inc_execution_pool_cache_messages_processed(processed);
                    debug!(
                        processed,
                        "SLAVE CACHE: Synced PoolCacheUpdates from JetStream (cold-path task)"
                    );
                }
            }
            Err(e) => {
                debug!(error = %e, "No new PoolCacheUpdate messages in JetStream");
            }
        }

        info_poll_counter = info_poll_counter.saturating_add(1);
        if info_poll_counter % 10 == 0 {
            refresh_pool_cache_consumer_metrics(&mut consumer).await;
        }

        tokio::task::yield_now().await;
    }
}

/// Cold-path task: WalletBalanceSnapshot JetStream → LockManager (last-value wins per mint).
async fn run_wallet_snapshot_consumer_task(
    ctx: Arc<ExecutionContext>,
    consumer: JetStreamPullConsumer,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    use futures::StreamExt;

    let mut consumer = consumer;
    let mut info_poll_counter: u32 = 0;

    loop {
        if *shutdown_rx.borrow() {
            info!("Wallet snapshot consumer task shutting down");
            break;
        }

        match consumer
            .fetch()
            .max_messages(EE_JETSTREAM_CONSUMER_BATCH_MAX)
            .expires(EE_JETSTREAM_CONSUMER_FETCH_EXPIRES)
            .messages()
            .await
        {
            Ok(mut messages) => {
                let mut batch: Vec<(async_nats::jetstream::Message, MarketEvent)> = Vec::new();
                while let Some(msg_result) = messages.next().await {
                    match msg_result {
                        Ok(msg) => match serde_json::from_slice::<MarketEvent>(&msg.payload) {
                            Ok(event) => batch.push((msg, event)),
                            Err(e) => {
                                debug!(error = %e, "Failed to deserialize wallet snapshot MarketEvent");
                                let _ = msg.ack().await;
                            }
                        },
                        Err(e) => {
                            debug!(error = %e, "JetStream wallet snapshot fetch returned error");
                        }
                    }
                }

                let mut last_index_by_mint: HashMap<String, usize> = HashMap::new();
                for (idx, (_, event)) in batch.iter().enumerate() {
                    if let MarketEventKind::WalletBalanceSnapshot { mint, .. } = &event.kind {
                        last_index_by_mint.insert(mint.clone(), idx);
                    }
                }

                // Last-value wins per mint within this batch only. Intermediate transitions
                // (e.g. >0→0→>0) are collapsed to the final snapshot; PositionAuthority uses
                // the same end-state and ignores SOL/WSOL for open-count (PA-2).
                for (idx, (msg, event)) in batch.into_iter().enumerate() {
                    let apply =
                        if let MarketEventKind::WalletBalanceSnapshot { mint, .. } = &event.kind {
                            last_index_by_mint.get(mint) == Some(&idx)
                        } else {
                            true
                        };
                    if apply {
                        ctx.apply_wallet_balance_snapshot_event(&event);
                    }
                    if let Err(e) = msg.ack().await {
                        debug!(error = %e, "Failed to ack wallet snapshot message");
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, "No new wallet snapshot messages in JetStream");
            }
        }

        info_poll_counter = info_poll_counter.saturating_add(1);
        if info_poll_counter % 10 == 0 {
            refresh_wallet_snapshot_consumer_metrics(&mut consumer).await;
        }

        tokio::task::yield_now().await;
    }
}

/// Cold-path task: WalletTxConfirmed JetStream → pending confirm waiters (bounded batches).
async fn run_wallet_tx_confirm_consumer_task(
    ctx: Arc<ExecutionContext>,
    wallet: Pubkey,
    mut consumer: Option<JetStreamPullConsumer>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    use futures::StreamExt;

    loop {
        if *shutdown_rx.borrow() {
            info!("Wallet TX confirm consumer task shutting down");
            break;
        }

        if consumer.is_none() {
            if let Some(ref nats) = ctx.nats {
                consumer = create_wallet_tx_confirm_live_consumer(nats, &wallet).await;
            }
            if consumer.is_none() {
                evict_stale_orphan_tx_confirms(
                    &ctx.recent_orphan_tx_confirms,
                    ORPHAN_TX_CONFIRM_TTL,
                );
                tokio::time::sleep(EE_WALLET_TX_CONFIRM_RECREATE_BACKOFF).await;
                continue;
            }
        }

        if let Some(ref mut active_consumer) = consumer {
            match active_consumer
                .fetch()
                .max_messages(EE_JETSTREAM_CONSUMER_BATCH_MAX)
                .expires(EE_JETSTREAM_CONSUMER_FETCH_EXPIRES)
                .messages()
                .await
            {
                Ok(mut messages) => {
                    while let Some(msg_result) = messages.next().await {
                        match msg_result {
                            Ok(msg) => {
                                match serde_json::from_slice::<MarketEvent>(&msg.payload) {
                                    Ok(event) => {
                                        if let MarketEventKind::WalletTxConfirmed {
                                            signature,
                                            err,
                                            ..
                                        } = event.kind
                                        {
                                            if let Some(slot) = event.slot {
                                                dispatch_wallet_tx_confirmed(
                                                    &ctx.pending_tx_confirms,
                                                    &ctx.recent_orphan_tx_confirms,
                                                    &signature,
                                                    slot,
                                                    err,
                                                );
                                            } else {
                                                warn!(
                                                    sig = %signature,
                                                    "WalletTxConfirmed missing event.slot; skipping dispatch"
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        TX_CONFIRM_DESERIALIZE_ERRORS_TOTAL
                                            .fetch_add(1, Ordering::Relaxed);
                                        warn!(
                                            error = %e,
                                            "Failed to deserialize WalletTxConfirmed MarketEvent"
                                        );
                                    }
                                }
                                if let Err(e) = msg.ack().await {
                                    debug!(
                                        error = %e,
                                        "Failed to ack wallet TX confirm message"
                                    );
                                }
                            }
                            Err(e) => {
                                debug!(
                                    error = %e,
                                    "JetStream wallet TX confirm fetch returned error"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!(error = %e, "No new wallet TX confirm messages in JetStream");
                }
            }
        }

        evict_stale_orphan_tx_confirms(&ctx.recent_orphan_tx_confirms, ORPHAN_TX_CONFIRM_TTL);

        tokio::task::yield_now().await;
    }
}

async fn drain_completed_intent_tasks(task_set: &mut tokio::task::JoinSet<()>) {
    while let Some(result) = task_set.try_join_next() {
        if let Err(e) = result {
            error!(error = %e, "Intent task panicked");
        }
    }
}

fn spawn_process_intent_task(
    task_set: &mut tokio::task::JoinSet<()>,
    ctx: Arc<ExecutionContext>,
    intent: TradeIntent,
) {
    let sem = Arc::clone(&ctx.intent_semaphore);
    let intent_id = intent.intent_id.clone();
    task_set.spawn(async move {
        let _permit = match sem.acquire().await {
            Ok(p) => p,
            Err(_) => {
                warn!(intent_id = %intent_id, "Intent semaphore closed, dropping intent");
                return;
            }
        };
        CONCURRENT_INTENTS_GAUGE.fetch_add(1, Ordering::Relaxed);
        let result = process_intent(&ctx, intent).await;
        CONCURRENT_INTENTS_GAUGE.fetch_sub(1, Ordering::Relaxed);
        if let Err(e) = result {
            error!(error = %e, "Failed to process intent");
        }
    });
}

fn dispatch_intent_from_channel(
    task_set: &mut tokio::task::JoinSet<()>,
    ctx: &Arc<ExecutionContext>,
    intent: TradeIntent,
    enqueue_tracker: &Arc<IntentChannelEnqueueTracker>,
) {
    let recv_ms = wall_clock_unix_ms_now();
    enqueue_tracker.take_and_record_channel_wait(&intent.intent_id, recv_ms);
    let queue_depth = dec_execution_intent_rx_queue_depth();
    if queue_depth >= INTENT_RX_QUEUE_DEPTH_WARN_THRESHOLD {
        warn!(
            queue_depth,
            intent_id = %intent.intent_id,
            "Intent RX queue depth at or above threshold; possible dispatch backlog"
        );
    }
    info!(
        intent_id = %intent.intent_id,
        source = %intent.source,
        "Received TradeIntent from NATS"
    );
    spawn_process_intent_task(task_set, Arc::clone(ctx), intent);
}

async fn shutdown_intent_dispatcher_tasks(
    task_set: &mut tokio::task::JoinSet<()>,
    ctx: &Arc<ExecutionContext>,
) {
    ctx.intent_semaphore.close();
    let shutdown_deadline = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(shutdown_deadline);
    loop {
        tokio::select! {
            result = task_set.join_next() => {
                match result {
                    Some(Err(e)) => error!(error = %e, "Intent task panicked during shutdown"),
                    Some(Ok(())) => {}
                    None => {
                        info!("All in-flight intents completed");
                        break;
                    }
                }
            }
            _ = &mut shutdown_deadline => {
                warn!(
                    remaining = task_set.len(),
                    "Shutdown timeout (60s), aborting remaining intent tasks"
                );
                task_set.abort_all();
                break;
            }
        }
    }
}

/// Dedicated hot-path loop: reads `intent_rx` and spawns `process_intent` without sharing
/// the main-loop `select!` with Pool/Wallet heartbeat work (prevents intent starvation).
async fn run_intent_dispatcher(
    mut intent_rx: tokio::sync::mpsc::Receiver<TradeIntent>,
    ctx: Arc<ExecutionContext>,
    enqueue_tracker: Arc<IntentChannelEnqueueTracker>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut task_set = tokio::task::JoinSet::new();
    let mut drain_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    drain_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if *shutdown_rx.borrow() {
            info!(
                in_flight = task_set.len(),
                "Intent dispatcher shutdown requested"
            );
            break;
        }

        tokio::select! {
            Some(intent) = intent_rx.recv() => {
                dispatch_intent_from_channel(&mut task_set, &ctx, intent, &enqueue_tracker);
            }
            _ = drain_interval.tick() => {
                drain_completed_intent_tasks(&mut task_set).await;
            }
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    info!(
                        in_flight = task_set.len(),
                        "Intent dispatcher shutdown requested"
                    );
                    break;
                }
            }
        }
    }

    shutdown_intent_dispatcher_tasks(&mut task_set, &ctx).await;
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

    // Golden Replay Mode: read intents from JSONL, write decisions to JSONL, no NATS/RPC.
    if args.replay {
        if let (Some(intents_path), Some(output_path)) = (&args.replay_intents, &args.replay_output)
        {
            run_golden_replay(intents_path, output_path).await;
            return Ok(());
        }
        error!("--replay requires --replay-intents and --replay-output");
        std::process::exit(1);
    }

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

    // Set readiness mode for /status (E2E blackbox)
    set_readiness_mode(if args.dry_run {
        1
    } else if args.simulate_only {
        3
    } else {
        0
    });

    // Start metrics server
    let metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr, MetricsComponent::ExecutionEngine).await {
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
            tier1_fee_percentile: fp.tier1_fee_percentile,
            tier1_fee_multiplier: fp.tier1_fee_multiplier,
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
        // confirm_commitment: default confirmed (faster confirmation; reorg risk). Use "finalized" in config for stricter finality.
        confirm_commitment: exec_eng_cfg
            .and_then(|e| e.confirm_commitment.clone())
            .unwrap_or_else(|| "confirmed".to_string()),
        rebroadcast_use_tpu: exec_eng_cfg
            .and_then(|e| e.rebroadcast_use_tpu)
            .unwrap_or_else(|| {
                exec_eng_cfg
                    .and_then(|e| e.tx_submission.as_ref())
                    .map(|t| t.tpu_enabled)
                    .unwrap_or(true)
            }),
        capital_lock_ttl_buffer_ms: exec_eng_cfg
            .and_then(|e| e.capital_lock_ttl_buffer_ms)
            .unwrap_or(10_000),
        publish_position_authority_kv: exec_eng_cfg
            .and_then(|e| e.publish_position_authority_kv)
            .unwrap_or(false),
        ..Default::default()
    };

    if exec_config.publish_position_authority_kv {
        warn!(
            "execution-engine publish_position_authority_kv=true: EE will write POSITION_AUTHORITY KV. \
             Stop position-manager to avoid split-brain (PA-6b rollback mode only)."
        );
    } else {
        info!("PositionAuthority JetStream KV writes disabled in execution-engine (position-manager is sole writer)");
    }

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

    // P166: flush each write so execution_results JSONL is visible on disk before process exit
    // (BufWriter default 8KB; small records stay buffered in release when flush_each_write=false).
    let decision_config = JsonlWriterConfig::new("decision_records")
        .with_log_dir(log_base.join("decisions"))
        .with_flush_each_write(true);
    let decision_writer = JsonlWriter::new(decision_config)?;

    // P172: same-day segment rotation so a single UTC file cannot grow unbounded (Grafana tail-read).
    let execution_config = JsonlWriterConfig::new("execution_results")
        .with_log_dir(log_base.join("executions"))
        .with_flush_each_write(true)
        .with_segment_rotation(SegmentRotationLimits::execution_results_from_env());
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

    // Load wallet keys (Single-Signer).
    //
    // Important constraint (see plan / architecture rules):
    // - No RPC reads for SOL/WSOL/token balances in runtime (simulation excluded).
    // - We therefore do NOT fetch balances here.
    // - Balances are learned via market-data events:
    //   - `WalletBalanceUpdate` (SOL + WSOL)
    //   - JetStream `WALLET_SNAPSHOT` (token balances)
    let (wallet_pubkey, initial_sol, initial_wsol) = if let Some(ref t) = treasury {
        let pubkey = t.pubkey();
        info!(
            wallet = %pubkey,
            initial_sol_lamports = args.initial_sol_lamports,
            "Wallet keys loaded; skipping startup balance RPC (event-driven balances)"
        );
        (Some(pubkey), args.initial_sol_lamports, None)
    } else {
        (None, args.initial_sol_lamports, None)
    };

    // Setup lock manager with real balance (SOL + WSOL)
    let lock_manager = LockManager::new(initial_sol);
    // Initialize WSOL balance if we found it
    if let Some(wsol) = initial_wsol {
        lock_manager.update_wallet_balances(initial_sol, Some(wsol));
    }
    info!(
        initial_sol = initial_sol,
        initial_wsol = ?initial_wsol,
        sol_balance = initial_sol as f64 / 1e9,
        wsol_balance = initial_wsol.map(|w| w as f64 / 1e9),
        "Lock manager initialized with wallet balances"
    );

    // Grafana "Available WSOL" shows actual WSOL only; 0 when no WSOL ATA exists.
    AVAILABLE_SOL_LAMPORTS.store(initial_wsol.unwrap_or(0), Ordering::Relaxed);

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
        let mut config = NatsConfig::new(&args.nats_url, "execution-engine");
        config.request_timeout = NatsConfig::request_timeout_from_env(180);
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            warn!(error = %e, "Failed to connect to NATS (continuing without)");
            None
        } else {
            info!(url = %args.nats_url, "Connected to NATS");
            set_readiness_nats_connected(true);
            Some(client)
        }
    };

    // P1: Determine initial values from snapshot (DoD K)
    // Architecture: execution-engine does NOT scan wallet via RPC (that's market-data's job).
    // Positions are tracked purely from snapshot + ExecutionResults.
    // If reconciliation is needed after manual sales/transfers, use market-data's
    // WalletBalanceSnapshot events consumed by momentum-bot (strategy plane).

    let (initial_day, initial_daily_loss, initial_decision_counter, initial_execution_counter) =
        if let Some(ref snap) = snapshot {
            if snap.is_same_day() {
                // Same day: restore all counters
                info!(
                    daily_loss = snap.daily_loss_lamports,
                    open_positions = snap.open_positions,
                    decision_counter = snap.decision_counter,
                    "Restored same-day state from snapshot"
                );
                (
                    chrono::NaiveDate::parse_from_str(&snap.day, "%Y-%m-%d")
                        .unwrap_or_else(|_| chrono::Utc::now().date_naive()),
                    snap.daily_loss_lamports,
                    snap.decision_counter,
                    snap.execution_counter,
                )
            } else {
                // New day: reset daily counters but keep counters for ID generation
                info!(
                    old_day = %snap.day,
                    "New day detected, resetting daily loss but keeping ID counters"
                );
                (
                    chrono::Utc::now().date_naive(),
                    0,                     // Reset daily loss
                    snap.decision_counter, // Keep for unique IDs across restarts
                    snap.execution_counter,
                )
            }
        } else {
            // Fresh start (no snapshot)
            (chrono::Utc::now().date_naive(), 0, 0, 0)
        };

    // Seed token balances AND SOL/WSOL from wallet snapshot stream (JetStream).
    // This also replaces the hardcoded 1 SOL default with the actual on-chain balance
    // from JetStream (persistent), avoiding the race condition with core NATS
    // WalletBalanceUpdate (fire-and-forget, can be missed if not yet subscribed).
    // The live consumer is created immediately after bootstrap (reused in the main loop)
    // so snapshots published during the long startup phase are not missed (DeliverPolicy::New).
    let mut wallet_snapshot_bootstrap_observed = 0usize;
    let mut wallet_snapshot_kinds: Vec<MarketEventKind> = Vec::new();
    let wallet_snapshot_bootstrap_consumer = if let (Some(ref nats_client), Some(wallet)) =
        (&nats, wallet_pubkey)
    {
        (wallet_snapshot_bootstrap_observed, wallet_snapshot_kinds) =
            match bootstrap_token_balances_from_wallet_snapshot(nats_client, &wallet, &lock_manager)
                .await
            {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(error = %e, "Wallet snapshot bootstrap: failed to seed token balances");
                    (0, Vec::new())
                }
            };
        // "Available WSOL" metric: always actual WSOL (0 when no ATA), never native SOL fallback.
        let wsol = lock_manager.available_wsol();
        AVAILABLE_SOL_LAMPORTS.store(wsol, Ordering::Relaxed);
        let total_native_sol = lock_manager.total_native_sol();
        let wsol = lock_manager.wsol_balance();
        WALLET_TOTAL_SOL_LAMPORTS.store(total_native_sol.saturating_add(wsol), Ordering::Relaxed);
        info!(
            available_wsol = wsol as f64 / 1e9,
            total_sol_wsol = (total_native_sol.saturating_add(wsol)) as f64 / 1e9,
            "Prometheus metrics refreshed after wallet snapshot bootstrap"
        );
        create_wallet_snapshot_live_consumer(
            nats_client,
            &wallet,
            wallet_snapshot_bootstrap_observed,
        )
        .await
    } else {
        None
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

    // Shutdown channel created early so pool-cache live consumer can spawn immediately
    // after bootstrap — no gap between bootstrap end and DeliverPolicy::New durable.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Bootstrap LivePoolCache from JetStream (state recovery after restart).
    // Immediately after bootstrap: create DeliverPolicy::New durable consumer and spawn
    // cold-path task (bootstrap ephemeral LastPerSubject consumer is dropped; no replay).
    if let (Some(ref nats_client), Some(ref cache)) = (&nats, &live_pool_cache) {
        match bootstrap_pool_cache_from_jetstream(nats_client, cache).await {
            Ok((pools_recovered, _bootstrap_consumer)) => {
                info!(
                    pools_recovered,
                    "SLAVE CACHE: State recovered from JetStream"
                );
            }
            Err(e) => {
                warn!(error = %e, "SLAVE CACHE: JetStream bootstrap failed (will rely on incremental updates)");
            }
        }

        if let Some(consumer) = create_pool_cache_live_consumer_for_ee(nats_client).await {
            let cache_spawn = Arc::clone(cache);
            let shutdown_rx_pool = shutdown_rx.clone();
            tokio::spawn(async move {
                run_pool_cache_consumer_task(cache_spawn, consumer, shutdown_rx_pool).await;
            });
            info!(
                stream = STREAM_NAME,
                "Pool cache live consumer task started immediately after bootstrap (no startup gap)"
            );
        }
    }

    // I-24d: No global Startup-Seeding/Discovery-Rebuild. market-data is Discovery authority.
    // Pool_accounts arrive via JetStream PoolCacheUpdate or Discovery Request/Reply on demand.

    let pending_tx_confirms: Arc<
        parking_lot::RwLock<
            std::collections::HashMap<String, tokio::sync::oneshot::Sender<WalletTxConfirmNotify>>,
        >,
    > = Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new()));
    let recent_orphan_tx_confirms: Arc<
        parking_lot::RwLock<std::collections::HashMap<String, OrphanTxConfirmEntry>>,
    > = Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new()));

    let publish_position_authority_kv = exec_config.publish_position_authority_kv;
    let position_authority_kv = tokio::sync::OnceCell::new();
    let position_authority_kv_publisher = if publish_position_authority_kv {
        nats.as_ref()
            .map_or(PositionAuthorityKvPublisher::disabled(), |n| {
                PositionAuthorityKvPublisher::spawn(
                    n.clone_for_spawned_publish(),
                    PositionAuthorityKvMetricsSink::None,
                )
            })
    } else {
        PositionAuthorityKvPublisher::disabled()
    };

    let mut ctx = ExecutionContext {
        run_id: run_id.clone(),
        rpc_url: args.rpc_url.clone(),
        helius_rpc_url,
        wallet_pubkey,
        treasury,
        config_snapshot_id: parking_lot::RwLock::new(exec_config.snapshot_id()),
        intent_semaphore: Arc::new(tokio::sync::Semaphore::new(
            exec_config.max_concurrent_intents as usize,
        )),
        pending_discovery_responses: Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        config: parking_lot::RwLock::new(exec_config),
        nats,
        decision_writer,
        execution_writer,
        burn_writer,
        lock_manager,
        position_authority: Arc::new(ParkingMutex::new(PositionAuthority::new())),
        position_authority_kv_publisher,
        log_base: log_base.clone(),
        decision_counter: std::sync::atomic::AtomicU64::new(initial_decision_counter),
        execution_counter: std::sync::atomic::AtomicU64::new(initial_execution_counter),
        // Risk tracking - restored from snapshot
        current_day: parking_lot::RwLock::new(initial_day),
        daily_loss_lamports: std::sync::atomic::AtomicI64::new(initial_daily_loss),
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
        // PR 5: Cached blockhash from Geyser blocks_meta (via market-data NATS)
        cached_blockhash: parking_lot::RwLock::new(None),
        // Option C: LivePoolCache - for zero-RPC quote calculation
        live_pool_cache: live_pool_cache.clone(),
        // PR3: JetStream WalletTxConfirmed waiters (market-data Geyser → JetStream)
        pending_tx_confirms,
        recent_orphan_tx_confirms,
        // WsolManager balance updates (from JetStream); set when WsolManager enabled
        wsol_balance_tx: None,
        wsol_pending_wrap: None,
        // Metrics
        intents_received: std::sync::atomic::AtomicU64::new(0),
        intents_rejected: std::sync::atomic::AtomicU64::new(0),
        sim_failures: std::sync::atomic::AtomicU64::new(0),
        tx_sent: std::sync::atomic::AtomicU64::new(0),
        arb_validated: std::sync::atomic::AtomicU64::new(0),
        arb_executed: std::sync::atomic::AtomicU64::new(0),
        replay_mode: false,
        pump_amm_hot_path_refresh_last: Arc::new(ParkingMutex::new(HashMap::new())),
    };

    if publish_position_authority_kv {
        if let Some(ref nats_client) = ctx.nats {
            if let Err(e) = reconcile_position_authority_kv_after_restart(
                nats_client,
                &ctx.position_authority,
                &position_authority_kv,
                &wallet_snapshot_kinds,
                PositionAuthorityKvMetricsSink::None,
            )
            .await
            {
                warn!(
                    error = %e,
                    "PositionAuthority KV startup reconcile failed (Momentum may see stale KV until next update)"
                );
            }
        }
    }
    if ctx.nats.is_some() {
        ctx.refresh_position_authority_metrics();
    }

    // Sync kill switch to global metric so control plane /status can display correct state after restarts
    KILL_SWITCH_ACTIVE.store(initial_kill_switch_active, Ordering::Relaxed);

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

    // Create WsolManager balance channel when treasury exists (JetStream → WsolManager)
    let wsol_pending_wrap_state = ctx
        .treasury
        .as_ref()
        .map(|_| Arc::new(PendingWrapState::new()));
    let wsol_balance_rx = if ctx.treasury.is_some() {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        ctx.wsol_balance_tx = Some(tx);
        ctx.wsol_pending_wrap = wsol_pending_wrap_state.clone();
        Some(rx)
    } else {
        None
    };

    let ctx: Arc<ExecutionContext> = Arc::new(ctx);

    // LivePoolCache is now synced via NATS from market-data (Single Source of Truth)
    // No longer spawning cache_geyser_task - execution-engine subscribes to PoolCacheUpdates instead

    // Publish initial gauge values immediately (before the first 30s heartbeat).
    ctx.refresh_position_authority_metrics();

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
            let ctx_for_kill_switch = Arc::clone(&ctx);
            let ctx_for_lock_sync = Arc::clone(&ctx);
            let lock_sync: ironcrab::execution::wsol_manager::WsolLockSyncCallback =
                Arc::new(move |wsol_lamports| {
                    ctx_for_lock_sync
                        .lock_manager
                        .update_wsol_only(wsol_lamports);
                });
            let pending_wrap = wsol_pending_wrap_state
                .clone()
                .expect("pending wrap state set when treasury exists");
            let wsol_manager = WsolManager::with_jsonl_writer(
                wsol_config.clone(),
                Arc::new(treasury.clone()),
                Arc::clone(&ctx.rpc),
                env!("CARGO_PKG_VERSION"),
                &run_id,
                Arc::clone(&wsol_writer),
            )
            .with_kill_switch(move || ctx_for_kill_switch.is_kill_switch_active())
            .with_pending_wrap_state(pending_wrap)
            .with_lock_manager_sync(lock_sync);
            let shutdown_rx_wsol = shutdown_rx.clone();

            let balance_rx = wsol_balance_rx.expect("wsol_balance_rx set when treasury exists");
            tokio::spawn(async move {
                if let Err(e) = wsol_manager.run(balance_rx, shutdown_rx_wsol).await {
                    error!(error = %e, "WsolManager task failed");
                }
            });

            info!(
                min_wsol = wsol_config.min_wsol_sol,
                target_wsol = wsol_config.target_wsol_sol,
                max_wsol = wsol_config.max_wsol_sol,
                dry_run = wsol_config.dry_run,
                "WsolManager background task started (balance updates via JetStream)"
            );

            // Seed WsolManager with bootstrap balances from LockManager (JetStream bootstrap).
            if let Some(ref tx) = ctx.wsol_balance_tx {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                let sol = ctx.lock_manager.total_native_sol();
                let wsol = ctx.lock_manager.wsol_balance();
                if let Err(e) = tx.send((sol, Some(wsol))).await {
                    warn!(error = %e, "Failed to send bootstrap balance to WsolManager");
                } else {
                    info!(
                        sol = sol,
                        wsol = wsol,
                        sol_sol = sol as f64 / 1e9,
                        wsol_sol = wsol as f64 / 1e9,
                        "Bootstrapped WsolManager with balance from JetStream"
                    );
                }
            }
        } else {
            info!("WsolManager disabled by config");
        }
    } else {
        debug!("WsolManager not started (no Treasury)");
    }

    // NOTE: LockManager balance updates now come from JetStream WalletBalanceSnapshot
    // Wallet snapshots are applied in a dedicated cold-path JetStream task (not main-loop tick).

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
                let mut janitor = AccountJanitor::with_router_and_jsonl(
                    janitor_config.clone(),
                    Arc::new(treasury.clone()),
                    Arc::clone(&ctx.rpc),
                    router,
                    Arc::clone(&janitor_writer),
                    run_id.clone(),
                );
                // Configure NATS for position blacklist (prevents selling momentum positions as dust)
                if let Some(ref nats) = ctx.nats {
                    janitor = janitor.with_nats_client(nats.client().clone());
                    info!("AccountJanitor: NATS configured for momentum position blacklist");
                }
                janitor
            } else {
                warn!("AccountJanitor: No Router available, swap_dust disabled");
                let mut janitor = AccountJanitor::with_jsonl_writer(
                    janitor_config.clone(),
                    Arc::new(treasury.clone()),
                    Arc::clone(&ctx.rpc),
                    Arc::clone(&janitor_writer),
                    run_id.clone(),
                );
                // Configure NATS even without Router (for future features)
                if let Some(ref nats) = ctx.nats {
                    janitor = janitor.with_nats_client(nats.client().clone());
                }
                janitor
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

    // Ensure TRADE_INTENTS JetStream stream exists (momentum_bot publishes here)
    if let Some(ref nats) = ctx.nats {
        if let Err(e) = ensure_trade_intents_stream(nats.client()).await {
            warn!(error = %e, "Failed to ensure TRADE_INTENTS stream; intents may be lost");
        }
        if let Err(e) = ensure_execution_results_stream(nats.client()).await {
            warn!(error = %e, "Failed to ensure EXECUTION_RESULTS stream; results may be lost");
        }
    }

    // JetStream consumer for TradeIntents (persistent, survives startup race)
    let intent_js_consumer = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;
        use ironcrab::nats::trade_intents_consumer_config;

        let jetstream = jetstream::new(nats.client().clone());
        match jetstream.get_stream(TRADE_INTENTS_STREAM_NAME).await {
            Ok(stream) => match stream
                .create_consumer(trade_intents_consumer_config())
                .await
            {
                Ok(consumer) => {
                    info!(
                        stream = TRADE_INTENTS_STREAM_NAME,
                        subject = TOPIC_TRADE_INTENTS,
                        "Subscribed to TradeIntents via JetStream (persistent)"
                    );
                    Some(consumer)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create trade intents consumer");
                    None
                }
            },
            Err(e) => {
                warn!(
                    error = %e,
                    stream = TRADE_INTENTS_STREAM_NAME,
                    "Failed to get trade intents stream"
                );
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
    let (intent_tx, intent_rx) = tokio::sync::mpsc::channel::<TradeIntent>(100);
    let (control_tx, mut control_rx) = tokio::sync::mpsc::channel::<ControlRequest>(10);
    let (config_tx, mut config_rx) = tokio::sync::mpsc::channel::<ConfigUpdate>(10);
    let intent_channel_enqueue_tracker = IntentChannelEnqueueTracker::new();

    // Spawn dedicated task for TradeIntents JetStream consumer
    if let Some(intent_consumer) = intent_js_consumer {
        let tx = intent_tx.clone();
        let enqueue_tracker = Arc::clone(&intent_channel_enqueue_tracker);
        tokio::spawn(async move {
            use futures::StreamExt;
            loop {
                match intent_consumer
                    .fetch()
                    .max_messages(10)
                    .expires(std::time::Duration::from_secs(5))
                    .messages()
                    .await
                {
                    Ok(mut messages) => {
                        while let Some(msg_result) = messages.next().await {
                            if let Ok(msg) = msg_result {
                                match serde_json::from_slice::<TradeIntent>(&msg.payload) {
                                    Ok(intent) => {
                                        let deser_done_ms = wall_clock_unix_ms_now();
                                        let intent_id = intent.intent_id.clone();
                                        if tx.send(intent).await.is_err() {
                                            warn!("TradeIntent channel closed, stopping JetStream consumer");
                                            return;
                                        }
                                        inc_execution_intent_rx_queue_depth();
                                        let enqueue_ms = wall_clock_unix_ms_now();
                                        record_execution_intent_jetstream_to_channel_ms(
                                            enqueue_ms.saturating_sub(deser_done_ms),
                                        );
                                        enqueue_tracker.record_enqueue(intent_id, enqueue_ms);
                                        if let Err(e) = msg.ack().await {
                                            warn!(error = %e, "Failed to ack trade intent");
                                        }
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "Failed to deserialize TradeIntent");
                                        let _ = msg.ack().await;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!(error = %e, "JetStream trade intents fetch returned (expected when no new messages)");
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

    // Subscription liveness: updated by tasks so /status reflects current contract state
    let control_sub_last_activity = Arc::new(std::sync::atomic::AtomicU64::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    ));
    let control_response_last_activity = Arc::new(std::sync::atomic::AtomicU64::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    ));

    // Spawn dedicated task for ControlRequests subscription
    if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONTROL_REQUESTS).await {
            Ok(mut control_sub) => {
                info!(
                    topic = TOPIC_CONTROL_REQUESTS,
                    "Subscribed to ControlRequests"
                );
                set_readiness_control_sub_active(true);
                let tx = control_tx.clone();
                let last_activity = Arc::clone(&control_sub_last_activity);
                tokio::spawn(async move {
                    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(5));
                    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tokio::select! {
                            Some(msg) = control_sub.next() => {
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
                                last_activity.store(
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs(),
                                    Ordering::Relaxed,
                                );
                            }
                            _ = heartbeat.tick() => {
                                last_activity.store(
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs(),
                                    Ordering::Relaxed,
                                );
                            }
                        }
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to ControlRequests");
            }
        }

        // I-24d: Subscribe to ControlResponses for Discovery Request/Reply correlation
        match nats.subscribe(TOPIC_CONTROL_RESPONSES).await {
            Ok(mut resp_sub) => {
                info!(
                    topic = TOPIC_CONTROL_RESPONSES,
                    "Subscribed to ControlResponses (Discovery Request/Reply)"
                );
                set_readiness_control_response_sub_active(true);
                let pending = Arc::clone(&ctx.pending_discovery_responses);
                let last_activity = Arc::clone(&control_response_last_activity);
                tokio::spawn(async move {
                    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(5));
                    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tokio::select! {
                            Some(msg) = resp_sub.next() => {
                                match msg.deserialize::<ControlResponse>() {
                                    Ok(resp) => {
                                        let request_id = resp.request_id.clone();
                                        let status_str = format!("{:?}", resp.status);
                                        let pool = resp.pool_address.clone();
                                        let mut map = pending.lock().await;
                                        if let Some(tx) = map.remove(&request_id) {
                                            info!(
                                                request_id = %request_id,
                                                status = %status_str,
                                                pool_address = ?pool,
                                                "I-24d Discovery: ControlResponse received, correlated and delivered"
                                            );
                                            let _ = tx.send(resp);
                                        } else {
                                            debug!(
                                                request_id = %request_id,
                                                "I-24d Discovery: ControlResponse received but no pending request (stale or other consumer)"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "Failed to deserialize ControlResponse");
                                    }
                                }
                                last_activity.store(
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs(),
                                    Ordering::Relaxed,
                                );
                            }
                            _ = heartbeat.tick() => {
                                last_activity.store(
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs(),
                                    Ordering::Relaxed,
                                );
                            }
                        }
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to ControlResponses");
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

    // PR1: Spawn dedicated task for TokenMintInfo events from MarketEvents subscription.
    // This populates the SLAVE LivePoolCache with mint decimals + token_program from Geyser,
    // so execution-engine never needs RPC calls to resolve decimals for tracked mints.
    // Pattern: Same as PriorityFeePercentiles – spawn task that writes directly into shared state.
    if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_MARKET_EVENTS).await {
            Ok(mut market_sub) => {
                info!(
                    topic = TOPIC_MARKET_EVENTS,
                    "Subscribed to MarketEvents for TokenMintInfo → SLAVE LivePoolCache decimals"
                );
                let cache_for_mint_info: Option<
                    Arc<ironcrab::execution::live_pool_cache::LivePoolCache>,
                > = ctx.live_pool_cache.as_ref().map(Arc::clone);
                let ctx_for_blockhash = Arc::clone(&ctx);
                tokio::spawn(async move {
                    let mut mint_info_count: u64 = 0;
                    while let Some(msg) = market_sub.next().await {
                        // Fast-path: only process if we have a cache
                        let cache = match cache_for_mint_info.as_ref() {
                            Some(c) => c,
                            None => continue,
                        };

                        // Deserialize and filter for TokenMintInfo events only
                        let event: MarketEvent = match msg.deserialize() {
                            Ok(e) => e,
                            Err(_) => continue, // Not a MarketEvent, skip silently
                        };

                        match &event.kind {
                            MarketEventKind::TokenMintInfo {
                                mint,
                                token_program,
                                decimals,
                                ..
                            } => {
                                if let Ok(mint_pk) = Pubkey::from_str(mint) {
                                    cache.set_mint_decimals(mint_pk, *decimals);

                                    // Also cache token_program (needed for ATA creation)
                                    if let Ok(program_pk) = Pubkey::from_str(token_program) {
                                        cache.update_mint_program(&mint_pk, program_pk);
                                    }

                                    mint_info_count += 1;
                                    if mint_info_count % 100 == 1 {
                                        debug!(
                                            total = mint_info_count,
                                            mint = %mint,
                                            decimals,
                                            "SLAVE CACHE: TokenMintInfo → decimals + token_program cached"
                                        );
                                    }
                                }
                            }
                            MarketEventKind::DexPoolAccounts {
                                dex,
                                pool_address,
                                accounts,
                                ..
                            } => {
                                // Populate PumpAmm pool_accounts in LivePoolCache.
                                // Without this, PumpAmm entries parsed from Geyser have empty pool_accounts,
                                // making them unusable for tx building (liquidation, sells).
                                if dex == "pump_amm" {
                                    if let Ok(pool_pk) = Pubkey::from_str(pool_address) {
                                        let parsed: Vec<Pubkey> = accounts
                                            .iter()
                                            .filter_map(|s| Pubkey::from_str(s).ok())
                                            .collect();
                                        if parsed.len() >= 12 {
                                            cache.set_pump_amm_pool_accounts(&pool_pk, parsed);
                                        } else {
                                            debug!(
                                                pool = %pool_address,
                                                dex = %dex,
                                                parsed_len = parsed.len(),
                                                raw_len = accounts.len(),
                                                "SLAVE CACHE: DexPoolAccounts skipped (too few valid accounts)"
                                            );
                                        }
                                    }
                                }
                                // Other DEX types don't need pool_accounts in LivePoolCache
                            }
                            MarketEventKind::LatestBlockhash {
                                blockhash,
                                slot,
                                block_height,
                            } => match solana_sdk::hash::Hash::from_str(blockhash) {
                                Ok(hash) => {
                                    *ctx_for_blockhash.cached_blockhash.write() =
                                        Some(CachedBlockhash {
                                            hash,
                                            slot: *slot,
                                            block_height: *block_height,
                                            received_at: std::time::Instant::now(),
                                        });
                                }
                                Err(e) => {
                                    warn!(
                                        blockhash,
                                        error = %e,
                                        "Failed to parse LatestBlockhash from NATS"
                                    );
                                }
                            },
                            _ => {
                                // Other MarketEvent types are handled by momentum-bot/arb-strategy
                            }
                        }
                    }
                    warn!("MarketEvents subscription ended (TokenMintInfo cache disabled)");
                });
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Failed to subscribe to MarketEvents (TokenMintInfo decimals will rely on RPC fallback)"
                );
            }
        }
    }

    // Cold-path JetStream consumers: WalletSnapshot, WalletTxConfirm (bounded batches).
    // Pool-cache consumer already spawned immediately after bootstrap (no startup gap).
    let wallet_snapshot_live_consumer = if let Some(consumer) = wallet_snapshot_bootstrap_consumer {
        info!(
            stream = WALLET_SNAPSHOT_STREAM_NAME,
            "Reusing early wallet snapshot consumer for live sync (no startup gap)"
        );
        Some(consumer)
    } else if let (Some(ref nats), Some(wallet)) = (&ctx.nats, ctx.wallet_pubkey) {
        create_wallet_snapshot_live_consumer(nats, &wallet, wallet_snapshot_bootstrap_observed)
            .await
    } else {
        None
    };

    let wallet_tx_confirm_live_consumer =
        if let (Some(ref nats), Some(wallet)) = (&ctx.nats, ctx.wallet_pubkey) {
            create_wallet_tx_confirm_live_consumer(nats, &wallet).await
        } else {
            None
        };

    if ctx.config.read().jetstream_tx_confirm_enabled
        && ctx.wallet_pubkey.is_some()
        && wallet_tx_confirm_live_consumer.is_none()
    {
        warn!(
            stream = WALLET_TX_CONFIRM_STREAM_NAME,
            "JetStream TX confirm enabled but WalletTxConfirmed consumer missing \
             (market-data may not be running or WALLET_TX_CONFIRM stream unavailable); \
             confirms will timeout"
        );
    }

    // E2E Readiness: consuming state paths (LockManager, LivePoolCache, JetStream consumers) initialized
    set_readiness_state_paths_initialized(true);

    // Hot-path isolation: intent dispatch in its own task (not in main-loop select! with Pool/Wallet).
    let intent_dispatcher_handle = {
        let ctx = Arc::clone(&ctx);
        let enqueue_tracker = Arc::clone(&intent_channel_enqueue_tracker);
        let shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            run_intent_dispatcher(intent_rx, ctx, enqueue_tracker, shutdown_rx).await;
        })
    };

    if let Some(consumer) = wallet_snapshot_live_consumer {
        let ctx = Arc::clone(&ctx);
        let shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            run_wallet_snapshot_consumer_task(ctx, consumer, shutdown_rx).await;
        });
    }

    if let Some(wallet) = ctx.wallet_pubkey {
        let ctx = Arc::clone(&ctx);
        let shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            run_wallet_tx_confirm_consumer_task(
                ctx,
                wallet,
                wallet_tx_confirm_live_consumer,
                shutdown_rx,
            )
            .await;
        });
    }

    loop {
        tokio::select! {
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
                            KILL_SWITCH_ACTIVE.store(active, Ordering::Relaxed);
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
                                if ctx.liquidation_in_progress.load(Ordering::SeqCst) {
                                    warn!("KillSwitch: Liquidation already in progress, ignoring duplicate request");
                                } else {
                                    let slippage = max_slippage_bps.unwrap_or(9900);
                                    let ttl = ttl_ms.unwrap_or(60_000);
                                    let ctx_spawn = Arc::clone(&ctx);
                                    tokio::spawn(async move {
                                        ExecutionContext::run_liquidation_job(
                                            ctx_spawn,
                                            slippage,
                                            ttl,
                                            reason,
                                        )
                                        .await;
                                    });
                                }
                            }
                        }
                        ControlRequestKind::ResetKillSwitch => {
                            ctx.kill_switch_active.store(false, Ordering::Relaxed);
                            KILL_SWITCH_ACTIVE.store(false, Ordering::Relaxed);
                            info!("Kill switch reset");
                            ctx.set_kill_switch_context(None);

                            // Persist immediately so restarts don't re-enable a previously active kill switch.
                            if let Err(e) = ctx.save_state() {
                                warn!(error = %e, "Failed to persist state after kill switch reset");
                            }

                            // Trigger WsolManager so it runs check_and_act immediately (can wrap now).
                            if let Some(ref tx) = ctx.wsol_balance_tx {
                                let sol = ctx.lock_manager.total_native_sol();
                                let wsol = ctx.lock_manager.wsol_balance();
                                if let Err(e) = tx.try_send((sol, Some(wsol))) {
                                    warn!(error = %e, "Failed to send balance to WsolManager after kill switch reset");
                                } else {
                                    info!(
                                        sol = sol as f64 / 1e9,
                                        wsol = wsol as f64 / 1e9,
                                        "Triggered WsolManager after kill switch reset (can wrap now)"
                                    );
                                }
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
                        // #[non_exhaustive]: unknown variants (e.g. EnsurePumpAmmPoolAccounts for market-data)
                        _ => {
                            debug!(kind = ?req.kind, "Ignoring ControlRequest for other target");
                        }
                    }
                }
            }

            _ = interval.tick() => {
                simulated_tick += 1;

                let interval_tick_block_started_ms = wall_clock_unix_ms_now();

                // Keep /ready fresh even when no intents flow.
                ironcrab::metrics::record_activity();

                // Refresh readiness from current runtime state (subscription heartbeats)
                let nats_connected = ctx.nats.as_ref().is_some_and(|n| n.is_connected());
                let control_sub_secs = control_sub_last_activity.load(Ordering::Relaxed);
                let control_response_secs = control_response_last_activity.load(Ordering::Relaxed);
                update_readiness_execution_engine_current(
                    nats_connected,
                    control_sub_secs,
                    control_response_secs,
                );

                record_execution_engine_interval_tick_duration_ms(
                    wall_clock_unix_ms_now().saturating_sub(interval_tick_block_started_ms),
                );

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
                    let expired_pre_send = ctx.lock_manager.cleanup_expired();
                    if expired_pre_send > 0 {
                        CAPITAL_LOCK_EXPIRED_RELEASED_TOTAL.fetch_add(
                            expired_pre_send as u64,
                            Ordering::Relaxed,
                        );
                    }

                    let (cap_locks, res_locks) = ctx.lock_manager.active_lock_count();
                    IN_FLIGHT_CAPITAL_RESERVATIONS.store(
                        ctx.lock_manager.in_flight_reservation_count() as u64,
                        Ordering::Relaxed,
                    );
                    let received = ctx.intents_received.load(std::sync::atomic::Ordering::Relaxed);
                    let rejected = ctx.intents_rejected.load(std::sync::atomic::Ordering::Relaxed);
                    let sim_fail = ctx.sim_failures.load(std::sync::atomic::Ordering::Relaxed);
                    let available_capital = ctx.lock_manager.available_trading_capital();

                    // Update Prometheus metrics
                    INTENTS_RECEIVED_TOTAL.store(received, Ordering::Relaxed);
                    INTENTS_REJECTED_TOTAL.store(rejected, Ordering::Relaxed);
                    SIMULATION_FAILURES_TOTAL.store(sim_fail, Ordering::Relaxed);
                    ctx.refresh_position_authority_metrics();
                    ACTIVE_CAPITAL_LOCKS.store(cap_locks as u64, Ordering::Relaxed);
                    ACTIVE_RESOURCE_LOCKS.store(res_locks as u64, Ordering::Relaxed);
                    // "Available WSOL" panel: actual WSOL only (0 when no ATA)
                    AVAILABLE_SOL_LAMPORTS.store(ctx.lock_manager.available_wsol(), Ordering::Relaxed);

                    // Wallet total = native SOL (available + locked) + WSOL
                    // Both values come from LockManager (fed by WalletBalanceUpdate events),
                    // ensuring they're consistent (same event source, same update timing).
                    // Do NOT use WSOL_BALANCE_LAMPORTS here - it's from WsolManager which
                    // updates independently and can be stale or 0 during startup.
                    let total_native_sol = ctx.lock_manager.total_native_sol();
                    let wsol = ctx.lock_manager.wsol_balance();
                    WALLET_TOTAL_SOL_LAMPORTS.store(total_native_sol.saturating_add(wsol), Ordering::Relaxed);

                    info!(
                        tick = simulated_tick,
                        intents_received = received,
                        intents_rejected = rejected,
                        sim_failures = sim_fail,
                        active_capital_locks = cap_locks,
                        active_resource_locks = res_locks,
                        available_wsol = ctx.lock_manager.available_wsol(),
                        trading_capital = available_capital,
                        native_sol = total_native_sol,
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
                info!("Shutdown signal received, draining intent dispatcher");
                let _ = shutdown_tx.send(true);
                break;
            }
        }
    }

    if let Err(e) = intent_dispatcher_handle.await {
        error!(error = %e, "Intent dispatcher task failed");
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

/// Load TradeIntents from a JSONL file (for Golden Replay)
fn load_intents_from_jsonl(path: &Path) -> Vec<TradeIntent> {
    let f = std::fs::File::open(path).expect("open intents file");
    let reader = std::io::BufReader::new(f);
    reader
        .lines()
        .map_while(Result::ok)
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(&l).expect("parse intent"))
        .collect()
}

/// Fixture-Daten für 6005-Retry Replay
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Liquidation6005Fixture {
    base_mint: String,
    pool_market: String,
    quote_mint: String,
    pool_accounts: Vec<String>,
    /// Token-Balance (raw) für base_mint, damit sell_token_balance-Check besteht.
    /// Muss >= required_capital.raw des Intent sein.
    #[serde(default)]
    initial_token_balance: Option<u64>,
}

/// Build minimal ExecutionContext for Golden Replay (no NATS, no RPC, no TX send).
/// Wenn `fixture` gesetzt: LivePoolCache mit PumpAmmState vorbelegen (6005-Retry-Test).
async fn build_replay_context(
    output_path: &Path,
    fixture: Option<Liquidation6005Fixture>,
) -> Result<ExecutionContext> {
    let run_id = Uuid::new_v4().to_string();
    let exec_config = ExecutionConfig {
        send_enabled: false,
        max_position_size_lamports: 500_000_000,
        daily_loss_limit_lamports: 5_000_000_000,
        max_open_positions: 5,
        max_slippage_bps: 500,
        ..ExecutionConfig::default()
    };
    let config_snapshot_id = exec_config.snapshot_id();

    let log_dir = output_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let prefix = output_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("golden_decisions")
        .to_string();

    let decision_config = JsonlWriterConfig::new(&prefix)
        .with_log_dir(&log_dir)
        .with_flush_each_write(true);
    let decision_writer = JsonlWriter::new(decision_config)?;

    let execution_config = JsonlWriterConfig::new("replay_exec")
        .with_log_dir(log_dir.join("executions"))
        .with_flush_each_write(true);
    let execution_writer = JsonlWriter::new(execution_config)?;

    let burn_config = JsonlWriterConfig::new("replay_burn").with_log_dir(log_dir.join("burns"));
    let burn_writer = JsonlWriter::new(burn_config)?;

    // Dummy RPC (never called for Phase 1: rejected_trade + sim_failed)
    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));

    let lock_manager = LockManager::new(1_000_000_000); // 1 SOL for replay

    let live_pool_cache = if let Some(ref f) = fixture {
        let base_mint = Pubkey::from_str(&f.base_mint).unwrap_or_else(|_| Pubkey::new_unique());
        let quote_mint = Pubkey::from_str(&f.quote_mint).unwrap_or_else(|_| Pubkey::new_unique());
        let pool_market = Pubkey::from_str(&f.pool_market).unwrap_or_else(|_| Pubkey::new_unique());
        let pool_accounts: Vec<Pubkey> = f
            .pool_accounts
            .iter()
            .filter_map(|s| Pubkey::from_str(s).ok())
            .collect();
        let (pool_base_token_account, pool_quote_token_account) = if pool_accounts.len() >= 6 {
            (pool_accounts[4], pool_accounts[5])
        } else {
            (Pubkey::new_unique(), Pubkey::new_unique())
        };
        let state = CachedPoolState::PumpAmm(PumpAmmState {
            base_mint,
            quote_mint,
            pool_base_token_account,
            pool_quote_token_account,
            base_reserve: Some(1_000_000_000_000),
            quote_reserve: Some(50_000_000_000),
            pool_accounts: pool_accounts.clone(),
            creator: None,
        });
        let cache = LivePoolCache::new();
        cache.upsert(pool_market, state, 0);
        if let Some(balance) = f.initial_token_balance {
            lock_manager.set_available_token_balance(f.base_mint.clone(), balance);
        }
        Some(Arc::new(cache))
    } else {
        None
    };

    let ctx = ExecutionContext {
        run_id: run_id.clone(),
        rpc_url: "replay".to_string(),
        helius_rpc_url: None,
        wallet_pubkey: Some(Pubkey::new_from_array([1u8; 32])), // Dummy for replay
        treasury: None,
        config: parking_lot::RwLock::new(exec_config),
        config_snapshot_id: parking_lot::RwLock::new(config_snapshot_id),
        nats: None,
        decision_writer,
        execution_writer,
        burn_writer,
        lock_manager,
        position_authority: Arc::new(ParkingMutex::new(PositionAuthority::new())),
        position_authority_kv_publisher: PositionAuthorityKvPublisher::disabled(),
        log_base: log_dir,
        decision_counter: std::sync::atomic::AtomicU64::new(0),
        execution_counter: std::sync::atomic::AtomicU64::new(0),
        current_day: parking_lot::RwLock::new(chrono::Utc::now().date_naive()),
        daily_loss_lamports: std::sync::atomic::AtomicI64::new(0),
        kill_switch_active: AtomicBool::new(false),
        liquidation_in_progress: AtomicBool::new(false),
        kill_switch_context: parking_lot::RwLock::new(None),
        burn_in_progress: AtomicBool::new(false),
        jito_client: None,
        bundles_submitted: std::sync::atomic::AtomicU64::new(0),
        bundles_confirmed: std::sync::atomic::AtomicU64::new(0),
        cross_dex_handler: None,
        rpc,
        address_lookup_table: None,
        tx_sender: None,
        dynamic_fee_percentiles: parking_lot::RwLock::new(None),
        cached_blockhash: parking_lot::RwLock::new(None),
        live_pool_cache,
        intent_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
        pending_discovery_responses: Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        wsol_balance_tx: None,
        wsol_pending_wrap: None,
        pending_tx_confirms: Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
        recent_orphan_tx_confirms: Arc::new(parking_lot::RwLock::new(
            std::collections::HashMap::new(),
        )),
        intents_received: std::sync::atomic::AtomicU64::new(0),
        intents_rejected: std::sync::atomic::AtomicU64::new(0),
        sim_failures: std::sync::atomic::AtomicU64::new(0),
        tx_sent: std::sync::atomic::AtomicU64::new(0),
        arb_validated: std::sync::atomic::AtomicU64::new(0),
        arb_executed: std::sync::atomic::AtomicU64::new(0),
        replay_mode: true,
        pump_amm_hot_path_refresh_last: Arc::new(ParkingMutex::new(HashMap::new())),
    };

    Ok(ctx)
}

/// Prüft ob Intents den 6005-Retry-Flow benötigen (SIMFAIL6005 + dex=pumpfun)
fn needs_6005_fixture(intents: &[TradeIntent]) -> bool {
    intents.iter().any(|i| {
        i.resources.output_mint == "SIMFAIL6005"
            && i.metadata.get("dex").map(|s| s.as_str()) == Some("pumpfun")
    })
}

/// Run Golden Replay: process intents from JSONL, write decisions to JSONL
async fn run_golden_replay(intents_path: &Path, output_path: &Path) {
    let intents = load_intents_from_jsonl(intents_path);
    info!(
        intents_count = intents.len(),
        intents_path = %intents_path.display(),
        output_path = %output_path.display(),
        "Golden Replay: starting"
    );

    let fixture = if needs_6005_fixture(&intents) {
        let fixture_path = intents_path
            .to_string_lossy()
            .replace("_intents.jsonl", "_fixture.json");
        let fixture_path = Path::new(&fixture_path);
        if fixture_path.exists() {
            match std::fs::read_to_string(fixture_path) {
                Ok(s) => match serde_json::from_str::<Liquidation6005Fixture>(&s) {
                    Ok(f) => {
                        info!(path = %fixture_path.display(), "Loaded 6005 fixture");
                        Some(f)
                    }
                    Err(e) => {
                        warn!(path = %fixture_path.display(), error = %e, "Failed to parse 6005 fixture");
                        None
                    }
                },
                Err(e) => {
                    warn!(path = %fixture_path.display(), error = %e, "Failed to read 6005 fixture");
                    None
                }
            }
        } else {
            warn!(path = %fixture_path.display(), "6005 fixture file not found");
            None
        }
    } else {
        None
    };

    let ctx = match build_replay_context(output_path, fixture).await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "Failed to build replay context");
            std::process::exit(1);
        }
    };

    let max_slippage_bps = 500u32;
    let mut decision_counter: u64 = 0;
    for intent in intents {
        decision_counter += 1;
        let result = process_intent(&ctx, intent.clone()).await;
        if let Err(e) = result {
            let is_6005 = is_6005_bonding_curve_complete(&e);
            let dex_pumpfun = intent.metadata.get("dex").map(|s| s.as_str()) == Some("pumpfun");
            if is_6005 && dex_pumpfun {
                if let Some(ref cache) = ctx.live_pool_cache {
                    // I-24d: allow_rpc_on_miss=false — no local PumpSwap discovery.
                    let pump_amm = PumpFunAmmDex::new_with_cache(
                        Arc::clone(&ctx.rpc),
                        Arc::clone(cache),
                        false,
                    );
                    if let Some(retry) = ctx
                        .try_6005_pumpfun_retry(&intent, &pump_amm, max_slippage_bps)
                        .await
                    {
                        if let Err(retry_e) = process_intent(&ctx, retry).await {
                            warn!(
                                mint = %intent.resources.input_mint,
                                error = %retry_e,
                                "6005-retry: PumpSwap AMM attempt also failed"
                            );
                        }
                    }
                } else {
                    warn!(error = %e, "6005-retry: no live_pool_cache (fixture missing?)");
                }
            } else {
                warn!(error = %e, "Replay process_intent failed");
            }
        }
    }

    info!(
        decisions_processed = decision_counter,
        "Golden Replay: finished"
    );
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

/// BUY max-open-positions gate: conservative count vs config limit (A.28 / Scope 49 / PA-3).
///
/// In-process only — no wallet scan / RPC.
/// `authority_current` is [`PositionAuthority::open_positions_count`] when available.
/// `lockmanager_current` is [`LockManager::count_non_zero_token_balances`].
/// `metadata_current` is strategy-reported (`current_open_positions`).
/// Effective count is the max of all available sources so neither ghost locks nor stale metadata
/// alone can bypass the limit.
fn max_open_positions_buy_gate(
    metadata_current: usize,
    authority_current: Option<usize>,
    lockmanager_current: usize,
    max_open: usize,
) -> (bool, String) {
    let (effective_current, source, authority_suffix) = match authority_current {
        Some(authority) => (
            metadata_current.max(authority).max(lockmanager_current),
            "position_authority(+lockmanager)",
            format!("authority_current={authority} "),
        ),
        None => (
            metadata_current.max(lockmanager_current),
            "lockmanager",
            "authority_unavailable=true ".to_string(),
        ),
    };
    let passed = effective_current < max_open;
    let details = format!(
        "{authority_suffix}lockmanager_current={lockmanager_current} metadata_current={metadata_current} effective_current={effective_current} {} max={max_open} source={source}",
        if passed { "<" } else { ">=" },
    );
    (passed, details)
}

/// SELL token-balance preflight: conservative min of LockManager and PositionAuthority (PA-4).
///
/// In-process only — no RPC. When authority does not track the mint, falls back to LockManager only.
fn sell_token_balance_gate(
    lockmanager_available: u64,
    authority_tradable: Option<u64>,
    required_raw: u64,
    mint: &str,
) -> (bool, String, u64) {
    let (effective, source) = match authority_tradable {
        Some(auth) => (
            lockmanager_available.min(auth),
            "position_authority(+lockmanager)",
        ),
        None => (lockmanager_available, "lockmanager_fallback"),
    };
    let passed = effective >= required_raw;
    let authority_display = authority_tradable
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let details = format!(
        "authority_tradable={authority_display} lockmanager_available={lockmanager_available} effective={effective} required={required_raw} mint={mint} source={source}"
    );
    (passed, details, effective)
}

/// Liquidation LockManager seed decision (PA-4): skip ghost seeds when authority is closed/zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiquidationSeedDecision {
    Skip,
    Seed(u64),
}

fn liquidation_lockmanager_seed_decision(
    authority: &PositionAuthority,
    mint: &str,
    rpc_balance_raw: u64,
) -> LiquidationSeedDecision {
    match authority.tradable_balance_raw(mint) {
        None => LiquidationSeedDecision::Seed(rpc_balance_raw),
        Some(0) => LiquidationSeedDecision::Skip,
        Some(auth_bal) => LiquidationSeedDecision::Seed(rpc_balance_raw.min(auth_bal)),
    }
}

/// Scale-in BUY adds to an existing mint position; it must not consume a new `max_open_positions` slot.
///
/// Returns skip details when `entry_kind=scale_in` **and** LockManager already tracks a non-zero
/// balance for `output_mint` (defense-in-depth vs metadata spoofing). Otherwise returns `None` and
/// the normal [`max_open_positions_buy_gate`] applies.
fn scale_in_max_open_positions_skip_details(
    entry_kind: Option<&str>,
    output_mint: &str,
    lock_manager: &LockManager,
) -> Option<String> {
    if entry_kind != Some("scale_in") {
        return None;
    }
    let existing_balance_raw = lock_manager.available_token_balance(output_mint);
    if existing_balance_raw == 0 {
        return None;
    }
    Some(format!(
        "skipped_for_scale_in (does not open new position; existing_balance_raw={existing_balance_raw})"
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PriorityFeeSelection {
    fee_micro_lamports: u64,
    source: &'static str,
}

fn tier1_dynamic_fee_from_percentiles(
    percentiles: &PriorityFeePercentiles,
    fee_policy: &FeePolicy,
) -> u64 {
    let base = match fee_policy.tier1_fee_percentile {
        75 => percentiles.p75,
        90 => percentiles.p90,
        25 => percentiles.p25,
        _ => percentiles.p50,
    };
    ((base as f64) * fee_policy.tier1_fee_multiplier) as u64
}

fn select_priority_fee_for_intent(
    intent: &TradeIntent,
    fee_policy: &FeePolicy,
    dynamic_percentiles: Option<&PriorityFeePercentiles>,
) -> PriorityFeeSelection {
    let static_fee = fee_policy.priority_fee_for_intent(intent);

    let Some(percentiles) = dynamic_percentiles else {
        return PriorityFeeSelection {
            fee_micro_lamports: static_fee,
            source: "static_floor",
        };
    };

    let dynamic_fee = match intent.tier {
        IntentTier::Tier0 => percentiles.tier0_recommended,
        IntentTier::Tier1 => tier1_dynamic_fee_from_percentiles(percentiles, fee_policy),
        IntentTier::Arb => percentiles.arb_recommended,
    };
    let effective_fee = dynamic_fee.max(static_fee);
    let source = if effective_fee > static_fee {
        "dynamic"
    } else {
        "static_floor"
    };

    tracing::debug!(
        intent_id = %intent.intent_id,
        tier = ?intent.tier,
        dynamic_fee_micro_lamports = dynamic_fee,
        static_fee_micro_lamports = static_fee,
        effective_fee_micro_lamports = effective_fee,
        source = source,
        p50 = percentiles.p50,
        p90 = percentiles.p90,
        sample_count = percentiles.sample_count,
        "Priority fee: max(dynamic, static) - static config as floor"
    );

    PriorityFeeSelection {
        fee_micro_lamports: effective_fee,
        source,
    }
}

#[inline]
fn confirmed_slot_delta_slots(slot_at_send: u64, confirmed_slot: u64) -> u64 {
    if slot_at_send == 0 {
        0
    } else {
        confirmed_slot.saturating_sub(slot_at_send)
    }
}

#[inline]
fn record_confirm_latency_metrics(
    start: std::time::Instant,
    slot_at_send: Option<u64>,
    confirmed_slot: Option<u64>,
) {
    let elapsed_ms = start.elapsed().as_millis() as u64;
    TX_CONFIRM_LATENCY_MS.store(elapsed_ms, Ordering::Relaxed);
    record_tx_send_to_confirm_ms(elapsed_ms);
    if let (Some(send_slot), Some(conf_slot)) = (slot_at_send, confirmed_slot) {
        record_tx_confirmed_slot_delta_slots(confirmed_slot_delta_slots(send_slot, conf_slot));
    }
}

/// After a successful send: `cached_blockhash.slot` minus intent metadata `slot` (Geyser chain position).
#[inline]
fn record_execution_slot_lag_at_send_if_applicable(ctx: &ExecutionContext, intent: &TradeIntent) {
    let Some(intent_slot) = intent
        .metadata
        .get("slot")
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|s| *s > 0)
    else {
        return;
    };
    let Some(cached) = ctx.cached_blockhash.read().clone() else {
        return;
    };
    if cached.slot >= intent_slot {
        record_execution_slot_lag_at_send_slots(cached.slot - intent_slot);
    }
}

/// Effective TTL for an intent: use intent field when > 0, else engine default.
fn effective_intent_ttl_ms(intent_ttl_ms: Option<u64>, default_ttl_ms: u64) -> u64 {
    intent_ttl_ms.filter(|t| *t > 0).unwrap_or(default_ttl_ms)
}

/// Pre-send capital-lock TTL: worst-case cold-path discovery + simulation + buffer.
///
/// Covers PumpSwap liquidation discovery (45s) + SLAVE wait (20s) without the 30s default
/// TTL racing ahead of an in-flight intent task (I-20).
fn compute_pre_send_capital_lock_ttl(config: &ExecutionConfig) -> std::time::Duration {
    let cold_discovery_ms = PUMPSWAP_LIQUIDATION_QUOTE_TIMEOUT_SECS
        .saturating_mul(1000)
        .saturating_add(PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS);
    let warm_discovery_ms = DISCOVERY_CACHE_WAIT_TIMEOUT_MS
        .saturating_add(PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS);
    let discovery_ms = cold_discovery_ms.max(warm_discovery_ms);
    let total_ms = discovery_ms
        .saturating_add(config.simulation_timeout_ms)
        .saturating_add(config.capital_lock_ttl_buffer_ms);
    std::time::Duration::from_millis(total_ms)
}

/// Returns true when the intent is past its TTL (stale).
fn intent_is_expired(
    now_unix_ms: u64,
    intent_ts_unix_ms: u64,
    intent_ttl_ms: Option<u64>,
    default_ttl_ms: u64,
) -> bool {
    let ttl = effective_intent_ttl_ms(intent_ttl_ms, default_ttl_ms);
    now_unix_ms > intent_ts_unix_ms.saturating_add(ttl)
}

fn sim_failure_reject_reason(error_code: Option<&str>) -> RejectReason {
    if error_code == Some("sim_timeout") {
        RejectReason::SimTimeout
    } else {
        RejectReason::SimFailed
    }
}

fn simulation_result_on_rpc_timeout() -> SimulationResult {
    SimulationResult {
        success: false,
        error_code: Some("sim_timeout".to_string()),
        logs_preview: None,
        compute_units_consumed: None,
    }
}

/// Process a single TradeIntent through the execution pipeline
async fn process_intent(ctx: &ExecutionContext, mut intent: TradeIntent) -> Result<()> {
    try_record_execution_intent_header_to_receive_ms(
        wall_clock_unix_ms_now(),
        intent.header.ts_unix_ms,
    );
    let process_intent_started = Instant::now();
    let _process_intent_duration = scopeguard::guard(process_intent_started, |t| {
        let us = t.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        record_execution_process_intent_us(us);
    });

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

    // === Check 2: TTL validity (before locks/planning — stale intents must not bind resources) ===
    // Golden replay fixtures carry historical header.ts_unix_ms; wall-clock TTL would expire them all.
    if ctx.replay_mode {
        checks.push(CheckResult {
            check_name: "ttl_valid".to_string(),
            passed: true,
            reason_code: None,
            details: Some("replay mode".to_string()),
        });
    } else {
        let now_unix_ms = wall_clock_unix_ms_now();
        let effective_ttl_ms = effective_intent_ttl_ms(intent.ttl_ms, config.intent_ttl_ms);
        if intent_is_expired(
            now_unix_ms,
            intent.header.ts_unix_ms,
            intent.ttl_ms,
            config.intent_ttl_ms,
        ) {
            let reason = RejectReason::TtlExpired;
            checks.push(CheckResult {
                check_name: "ttl_valid".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "now_ms={now_unix_ms} intent_ts_ms={} ttl_ms={effective_ttl_ms}",
                    intent.header.ts_unix_ms
                )),
            });
            return emit_expired_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        checks.push(CheckResult {
            check_name: "ttl_valid".to_string(),
            passed: true,
            reason_code: None,
            details: Some(format!(
                "now_ms={now_unix_ms} intent_ts_ms={} ttl_ms={effective_ttl_ms}",
                intent.header.ts_unix_ms
            )),
        });
    }

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
    // Skip for ALL sells - strategies like momentum-bot may need high slippage to exit positions
    // (e.g., exit_max_slippage_bps=9500 for emergency exits at any price)
    // Skip for market orders - exact SOL in, min tokens out = 1 (Custom(6002) on-chain)
    let is_market_order = intent
        .metadata
        .get("market_order")
        .map(|v| v == "true")
        .unwrap_or(false);
    if intent.side == TradeSide::Sell || is_market_order {
        checks.push(CheckResult {
            check_name: "max_slippage".to_string(),
            passed: true,
            reason_code: None,
            details: Some(if is_market_order {
                "skipped_for_market_order".to_string()
            } else {
                "skipped_for_sell".to_string()
            }),
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
    // PA-3: primary count from PositionAuthority; LockManager + strategy metadata kept
    // conservatively (max of all three). In-process only — no wallet scan / RPC.
    // Scale-in BUY (`entry_kind=scale_in`) skips this gate when LockManager already holds the mint
    // (P179): adds to an existing probe position, does not open a new token mint slot.
    if intent.side == TradeSide::Buy {
        let entry_kind = intent.metadata.get("entry_kind").map(|s| s.as_str());
        if let Some(skip_details) = scale_in_max_open_positions_skip_details(
            entry_kind,
            &intent.resources.output_mint,
            &ctx.lock_manager,
        ) {
            checks.push(CheckResult {
                check_name: "max_open_positions".to_string(),
                passed: true,
                reason_code: None,
                details: Some(skip_details),
            });
        } else {
            let metadata_current = intent
                .metadata
                .get("current_open_positions")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            let authority_current = ctx.get_position_authority_open_positions_count();
            let lockmanager_current = ctx.lock_manager.count_non_zero_token_balances();
            let (passed, details) = max_open_positions_buy_gate(
                metadata_current,
                authority_current,
                lockmanager_current,
                config.max_open_positions,
            );

            if !passed {
                let reason = RejectReason::RiskMaxOpenPositions;
                checks.push(CheckResult {
                    check_name: "max_open_positions".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some(if entry_kind == Some("scale_in") {
                        format!("{details}; scale_in_bypass_denied_no_existing_balance")
                    } else {
                        details
                    }),
                });
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
            checks.push(CheckResult {
                check_name: "max_open_positions".to_string(),
                passed: true,
                reason_code: None,
                details: Some(details),
            });
        }
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
    // sell_balance_hint: passed to tx_builder so CloseAccount is only added for full sells.
    let sell_balance_hint: Option<(u64, u64)> = if intent.side == TradeSide::Sell {
        let mint_str = intent.resources.input_mint.clone();
        let required_raw = intent.required_capital.raw;

        // This MUST be RPC-free. Balances are learned from market-data via JetStream wallet snapshots.
        if ctx.wallet_pubkey.is_none() {
            let reason = RejectReason::InternalError;
            checks.push(CheckResult {
                check_name: "sell_token_balance".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some("wallet_pubkey_unavailable (no keys loaded)".to_string()),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }

        let lockmanager_available = ctx.lock_manager.available_token_balance(&mint_str);
        let authority_tradable = {
            let pa = ctx.position_authority.lock();
            pa.tradable_balance_raw(&mint_str)
        };
        let (passed, details, effective_raw) = sell_token_balance_gate(
            lockmanager_available,
            authority_tradable,
            required_raw,
            &mint_str,
        );
        if !passed {
            let reason = RejectReason::SimInsufficientBalance;
            checks.push(CheckResult {
                check_name: "sell_token_balance".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(details),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }

        checks.push(CheckResult {
            check_name: "sell_token_balance".to_string(),
            passed: true,
            reason_code: None,
            details: Some(details),
        });
        Some((effective_raw, required_raw))
    } else {
        None
    };

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
    let priority_fee_selection = ctx.get_priority_fee_for_intent(&intent, &effective_fee_policy);
    let priority_fee = priority_fee_selection.fee_micro_lamports;
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
        details: Some(format!(
            "priority_fee_micro_lamports={} source={}",
            priority_fee, priority_fee_selection.source
        )),
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
                total_cost,
                base_fee,
                priority_fee_lamports,
                effective_fee_policy.max_tx_cost_lamports
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

    let pre_send_lock_ttl = compute_pre_send_capital_lock_ttl(&config);
    let mut locked_resources = 0u64;
    for pool in &intent.resources.pools {
        match ctx.lock_manager.try_lock_resource_with_ttl(
            holder.clone(),
            pool,
            ResourceType::Pool,
            Some(pre_send_lock_ttl),
        ) {
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

    // No global per-mint resource locks. Amount-scoped `try_lock_capital` below
    // is the canonical protection for consumed wallet balances (BUY: SOL/WSOL
    // spend, SELL: input token). Locking both `input_mint` and `output_mint` as
    // `ResourceType::Mint` serialized unrelated intents (e.g. in-flight BUY
    // vs STOP_LOSS SELL) whenever they shared a route mint such as WSOL, even
    // when the second leg only *receives* that mint.

    checks.push(CheckResult {
        check_name: "resource_locks".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!("locked={}", locked_resources)),
    });

    // === Check 5: Capital lock (BUY: trading WSOL after first WSOL snapshot, else native SOL; SELL: input tokens) ===
    let lock_result = if intent.side == TradeSide::Buy {
        ctx.lock_manager.try_lock_capital_with_ttl(
            holder,
            intent.required_capital.raw,
            std::collections::HashMap::new(),
            Some(pre_send_lock_ttl),
        )
    } else {
        let mut tokens = std::collections::HashMap::new();
        tokens.insert(
            intent.resources.input_mint.clone(),
            intent.required_capital.raw,
        );
        ctx.lock_manager
            .try_lock_capital_with_ttl(holder, 0, tokens, Some(pre_send_lock_ttl))
    };

    match lock_result {
        LockResult::Acquired => {
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: true,
                reason_code: None,
                details: Some(if intent.side == TradeSide::Buy {
                    if ctx
                        .lock_manager
                        .capital_lock_reserves_trading_wsol(&intent.intent_id)
                        .unwrap_or(false)
                    {
                        format!(
                            "buy:reserve_lamports_from_wsol_ata={}",
                            intent.required_capital.raw
                        )
                    } else {
                        format!(
                            "buy:reserve_lamports_from_native_sol_fallback={}",
                            intent.required_capital.raw
                        )
                    }
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

    // Golden Replay: 6005 (BondingCurveComplete) simulieren → löst Err aus,
    // damit Aufrufer Retry-Logik ausführen kann.
    // Wichtig: SimFailed-Decision VOR return Err schreiben, damit find_replay_output_file
    // mindestens eine Decision-Datei findet (auch wenn Retry scheitert).
    if ctx.replay_mode
        && intent.resources.output_mint == "SIMFAIL6005"
        && intent.metadata.get("dex").map(|s| s.as_str()) == Some("pumpfun")
    {
        checks.push(CheckResult {
            check_name: "simulation".to_string(),
            passed: false,
            reason_code: Some(RejectReason::SimFailed.to_string()),
            details: Some("Simulation failed: Custom(6005)".to_string()),
        });
        let sim_result = SimulationResult {
            success: false,
            error_code: Some("Simulation failed: Custom(6005)".into()),
            logs_preview: None,
            compute_units_consumed: None,
        };
        let _ = emit_sim_failed_decision(
            ctx,
            decision_id,
            &intent,
            checks,
            "replay-simfail-6005".to_string(),
            sim_result,
        )
        .await;
        ctx.lock_manager.release_locks(&intent.intent_id);
        return Err(anyhow::anyhow!("Simulation failed: Custom(6005)"));
    }

    // Golden Replay: Early exit for SIMFAIL intents (avoids TX build + simulate)
    if ctx.replay_mode && intent.resources.output_mint.starts_with("SIMFAIL") {
        let sim_result = SimulationResult {
            success: false,
            error_code: Some("Custom program error: 0x1".into()),
            logs_preview: None,
            compute_units_consumed: None,
        };
        checks.push(CheckResult {
            check_name: "simulation".to_string(),
            passed: false,
            reason_code: Some(RejectReason::SimFailed.to_string()),
            details: sim_result.error_code.clone(),
        });
        ctx.lock_manager.release_locks(&intent.intent_id);
        return emit_sim_failed_decision(
            ctx,
            decision_id,
            &intent,
            checks,
            "replay-simfail".to_string(),
            sim_result,
        )
        .await;
    }

    // Golden Replay: Early exit for SIMSUCC intents (avoids TX build + simulate; reaches send_disabled)
    if ctx.replay_mode && intent.resources.output_mint.starts_with("SIMSUCC") {
        checks.push(CheckResult {
            check_name: "simulation".to_string(),
            passed: true,
            reason_code: None,
            details: Some("CU consumed: Some(150000)".to_string()),
        });
        let mut checks = checks;
        checks.push(CheckResult {
            check_name: "send_enabled".to_string(),
            passed: false,
            reason_code: Some("send_disabled".to_string()),
            details: Some("execution-engine config.send_enabled=false".to_string()),
        });
        ctx.record_intent_rejected();
        INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        ctx.lock_manager.release_locks(&intent.intent_id);
        let sim_result = SimulationResult {
            success: true,
            error_code: None,
            logs_preview: None,
            compute_units_consumed: Some(150_000),
        };
        let mut input_snapshots = build_input_snapshots(&intent);
        input_snapshots.insert("fee_policy".to_string(), "standard".to_string());
        let decision = DecisionRecord {
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
            plan_hash: Some("replay-simsucc".to_string()),
            simulate: Some(sim_result),
            send: None,
            outcome: DecisionOutcome::Rejected,
            config_snapshot_id: None,
            input_snapshots,
        };
        ctx.decision_writer.write(&decision)?;
        if let Some(ref nats) = ctx.nats {
            nats.publish(TOPIC_DECISION_RECORDS, &decision).await?;
        }
        return Ok(());
    }

    // === Cross-DEX Arbitrage Detection (if applicable) ===
    let is_cross_dex_arb = CrossDexHandler::is_cross_dex_arb_intent(&intent);

    // Planned tx (RS-2.1): deterministic tx plan + plan_hash
    // NOTE: Cross-DEX arb intents are NOT single-swap plans and therefore must not go through
    // tx_builder::build_tx_plan (which requires metadata.dex and pools_len==1).
    let checks_len_before_tx_plan_build = checks.len();
    let mut pump_amm_preplan_discovery_attempted = false;
    let mut pump_amm_recovery_attempted = false;
    let mut pumpfun_bonding_recovery_attempted = false;
    let mut orca_recovery_attempted = false;
    let mut multi_pool_tx_plan_fallback_attempted = false;
    let (
        tx_plan,
        plan_hash_str,
        sim_result,
        requires_bundle,
        bundle_tip_ix,
        wallet_pubkey,
        bundle_tip_lamports,
    ) = 'tx_plan_retry: loop {
        if pump_amm_preplan_discovery_attempted
            || pump_amm_recovery_attempted
            || pumpfun_bonding_recovery_attempted
            || orca_recovery_attempted
            || multi_pool_tx_plan_fallback_attempted
        {
            checks.truncate(checks_len_before_tx_plan_build);
        }
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
                        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason)
                            .await;
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
                        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason)
                            .await;
                    }
                };

                // Include compute budget ixs so simulation matches send (and CU limit is sufficient).
                // P2: Use dynamic priority fees if available from Geyser
                let compute_units = effective_fee_policy.compute_units_for_intent(&intent);
                let micro_lamports_per_cu = ctx
                    .get_priority_fee_for_intent(&intent, &effective_fee_policy)
                    .fee_micro_lamports;

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
                // Scope 35b / A.43: cold-path PumpSwap with pool hint + empty `resources.accounts`
                // dies in `build_tx_plan` before simulation if the hint pool is missing from SLAVE
                // cache or `pool_accounts` are too short. Run bounded I-24d discovery **before** the
                // first plan build (same mechanism as post-sim recovery, different placement).
                if !ctx.replay_mode
                    && !pump_amm_preplan_discovery_attempted
                    && is_cold_path_recovery_sell(&intent)
                    && intent.metadata.get("dex").map(|s| s.as_str()) == Some("pump_amm")
                    && intent.resources.accounts.is_empty()
                    && intent.resources.pools.len() == 1
                {
                    if let (Ok(_base_mint_pk), Ok(pool_pk)) = (
                        Pubkey::from_str(&intent.resources.input_mint),
                        Pubkey::from_str(&intent.resources.pools[0]),
                    ) {
                        let cache_ok_for_builder = ctx.live_pool_cache.as_ref().is_some_and(|c| {
                            pump_amm_hint_pool_cache_usable_for_tx_plan_builder(c, &pool_pk)
                        });
                        if !cache_ok_for_builder {
                            let pool_hint_str = intent.resources.pools[0].as_str();
                            match ctx
                                .request_discovery_and_wait(
                                    &intent.resources.input_mint,
                                    Some(pool_hint_str),
                                    true,
                                )
                                .await
                            {
                                DiscoveryRequestOutcome::Ok => {
                                    if let Some(cache) = ctx.live_pool_cache.as_ref() {
                                        if wait_for_pump_amm_pool_hint_ready_for_tx_plan_builder(
                                            cache,
                                            &pool_pk,
                                            PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                                            DISCOVERY_CACHE_POLL_INTERVAL_MS,
                                        )
                                        .await
                                        {
                                            pump_amm_preplan_discovery_attempted = true;
                                            warn!(
                                                intent_id = %intent.intent_id,
                                                pool = %pool_pk,
                                                "PumpSwap cold-path pre-plan: EnsurePumpAmmPoolAccounts + SLAVE cache wait OK — retrying plan/sim loop once"
                                            );
                                            continue;
                                        }
                                    }
                                }
                                DiscoveryRequestOutcome::NotFound => {
                                    warn!(
                                        intent_id = %intent.intent_id,
                                        pool = %pool_hint_str,
                                        "PumpSwap cold-path pre-plan: EnsurePumpAmmPoolAccounts NotFound — proceeding to plan build (expected UnsupportedIntent if still missing)"
                                    );
                                }
                                DiscoveryRequestOutcome::Error(ref msg) => {
                                    warn!(
                                        intent_id = %intent.intent_id,
                                        pool = %pool_hint_str,
                                        error = %msg,
                                        "PumpSwap cold-path pre-plan: EnsurePumpAmmPoolAccounts error — proceeding to plan build"
                                    );
                                }
                                DiscoveryRequestOutcome::Timeout => {
                                    warn!(
                                        intent_id = %intent.intent_id,
                                        pool = %pool_hint_str,
                                        "PumpSwap cold-path pre-plan: EnsurePumpAmmPoolAccounts reply timeout — proceeding to plan build"
                                    );
                                }
                            }
                        }
                    }
                }

                // Option C: Pass LivePoolCache for zero-RPC quote calculation
                match tx_builder::build_tx_plan(
                    &intent,
                    wallet_pubkey,
                    Arc::clone(&ctx.rpc),
                    ctx.live_pool_cache.as_ref(),
                    sell_balance_hint,
                    intent.origin_type == IntentOrigin::ExecutionMevB,
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
                        if is_cold_path_recovery_sell(&intent)
                            && take_next_multi_pool_buildable_fallback_route(&mut intent)
                        {
                            multi_pool_tx_plan_fallback_attempted = true;
                            info!(
                                intent_id = %intent.intent_id,
                                details = %u.details,
                                dex = %intent
                                    .metadata
                                    .get("dex")
                                    .map(|s| s.as_str())
                                    .unwrap_or("?"),
                                "multi_pool: tx_plan UnsupportedIntent — switching to next pre-filtered buildable route, retrying plan build"
                            );
                            continue 'tx_plan_retry;
                        }
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

        // P184m: regular momentum PumpSwap SELL — skip simulation when SLAVE quote is not ready
        // (avoids Custom 6004 storm on degenerate reserves; I-9: never send without successful sim).
        if !ctx.replay_mode {
            if let Some(detail) =
                pump_amm_hot_path_quote_not_ready_detail(&intent, ctx.live_pool_cache.as_deref())
            {
                let pool_hint_str =
                    pump_amm_pool_market_hint_pk(&intent, ctx).map(|p| p.to_string());
                let base_mint_parse = Pubkey::from_str(&intent.resources.input_mint);
                let refresh_decision = match base_mint_parse {
                    Ok(pk) => try_pump_amm_hot_path_refresh_publish(
                        &ctx.pump_amm_hot_path_refresh_last,
                        pk,
                        Instant::now(),
                    ),
                    Err(_) => PumpAmmHotPathRefreshDecision::Publish,
                };
                if matches!(refresh_decision, PumpAmmHotPathRefreshDecision::Publish) {
                    if let Some(ref nats) = ctx.nats {
                        ExecutionContext::fire_pump_amm_pool_accounts_refresh_async(
                            nats,
                            ctx.run_id.clone(),
                            intent.resources.input_mint.clone(),
                            pool_hint_str,
                            Arc::clone(&ctx.pump_amm_hot_path_refresh_last),
                            base_mint_parse.ok(),
                        );
                    }
                }
                let reason = RejectReason::QuoteUnavailable;
                checks.push(CheckResult {
                    check_name: "pump_amm_quote_ready".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some(detail),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
        }

        // === Simulate (P0: simulate-gated) ===
        info!(intent_id = %intent.intent_id, "Running simulation");

        let sim_result = simulate_transaction(ctx, wallet_pubkey, &tx_plan_for_sim).await;

        if sim_result.success {
            break (
                tx_plan,
                plan_hash_str,
                sim_result,
                requires_bundle,
                bundle_tip_ix,
                wallet_pubkey,
                bundle_tip_lamports,
            );
        }

        // Regular PumpSwap SELL hot path: on structural sim failure, trigger async pool_accounts refresh
        // (no wait, no retry this intent). Cold-path recovery (`is_cold_path_recovery_sell`: liquidation,
        // kill-switch, or explicit `sell_all=true`) uses the synchronous wait + one tx rebuild retry below.
        if !ctx.replay_mode
            && !is_liquidation_sell
            && is_regular_momentum_hot_path_sell(&intent)
            && intent.metadata.get("dex").map(|s| s.as_str()) == Some("pump_amm")
            && is_pump_amm_structural_sim_error(sim_result.error_code.as_deref())
        {
            let pool_hint_str = pump_amm_pool_market_hint_pk(&intent, ctx).map(|p| p.to_string());
            let base_mint_parse = Pubkey::from_str(&intent.resources.input_mint);
            let refresh_decision = match base_mint_parse {
                Ok(pk) => try_pump_amm_hot_path_refresh_publish(
                    &ctx.pump_amm_hot_path_refresh_last,
                    pk,
                    Instant::now(),
                ),
                Err(_) => {
                    debug!(
                        intent_id = %intent.intent_id,
                        mint = %intent.resources.input_mint,
                        "PumpSwap regular SELL: base_mint not a valid pubkey; async refresh cooldown not applied"
                    );
                    PumpAmmHotPathRefreshDecision::Publish
                }
            };

            match &refresh_decision {
                PumpAmmHotPathRefreshDecision::Publish => {
                    PUMPSWAP_HOT_PATH_HEALING_TRIGGER_TOTAL.fetch_add(1, Ordering::Relaxed);
                }
                PumpAmmHotPathRefreshDecision::Suppress { .. } => {
                    PUMPSWAP_HOT_PATH_HEALING_COOLDOWN_SUPPRESSED_TOTAL
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            match refresh_decision {
                PumpAmmHotPathRefreshDecision::Publish => {
                    warn!(
                        intent_id = %intent.intent_id,
                        mint = %intent.resources.input_mint,
                        pool_hint = ?pool_hint_str,
                        sim_error = ?sim_result.error_code,
                        "PumpSwap regular SELL: simulation structural error (6013/6023/Overflow family); triggering async non-blocking EnsurePumpAmmPoolAccounts force_refresh to market-data (no wait, no retry this intent)"
                    );
                    if let Some(ref nats) = ctx.nats {
                        ExecutionContext::fire_pump_amm_pool_accounts_refresh_async(
                            nats,
                            ctx.run_id.clone(),
                            intent.resources.input_mint.clone(),
                            pool_hint_str,
                            Arc::clone(&ctx.pump_amm_hot_path_refresh_last),
                            base_mint_parse.ok(),
                        );
                    } else {
                        PUMPSWAP_HOT_PATH_HEALING_SKIPPED_NO_NATS_TOTAL
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(
                            intent_id = %intent.intent_id,
                            mint = %intent.resources.input_mint,
                            "PumpSwap regular SELL: async healing skipped — NATS not connected"
                        );
                    }
                }
                PumpAmmHotPathRefreshDecision::Suppress { age, remaining } => {
                    warn!(
                        intent_id = %intent.intent_id,
                        mint = %intent.resources.input_mint,
                        pool_hint = ?pool_hint_str,
                        sim_error = ?sim_result.error_code,
                        cooldown_secs = PUMP_AMM_HOT_PATH_REFRESH_COOLDOWN.as_secs(),
                        age_ms = age.as_millis(),
                        remaining_ms = remaining.as_millis(),
                        "PumpSwap regular SELL: structural sim error again; suppressing duplicate async EnsurePumpAmmPoolAccounts publish (hot-path cooldown, no NATS publish)"
                    );
                }
            }
        }

        // PumpSwap SELL: stale/wrong pool_accounts (creator vault, protocol fee recipient, …) → e.g. Custom(6023), Custom(6013).
        // Cold-path recovery: one EnsurePumpAmmPoolAccounts with force_refresh (RPC market parse).
        // Gate: `is_cold_path_recovery_sell` — liquidation, kill-switch, or explicit `sell_all=true` (not general momentum SELLs).
        // Liquidation/kill-switch: any sim fail may indicate stale accounts (also 6004 / slippage); sell-all-only keeps structural gate.
        if !pump_amm_recovery_attempted
            && is_cold_path_recovery_sell(&intent)
            && intent.metadata.get("dex").map(|s| s.as_str()) == Some("pump_amm")
            && cold_path_dex_sim_failure_triggers_discovery_recovery(
                &intent,
                sim_result.error_code.as_deref(),
                is_pump_amm_structural_sim_error,
            )
        {
            if Pubkey::from_str(&intent.resources.input_mint).is_err() {
                break (
                    tx_plan,
                    plan_hash_str,
                    sim_result,
                    requires_bundle,
                    bundle_tip_ix,
                    wallet_pubkey,
                    bundle_tip_lamports,
                );
            }
            let pool_pk = pump_amm_pool_market_hint_pk(&intent, ctx);
            if let Some(pool_pk) = pool_pk {
                let before_snap = ctx
                    .live_pool_cache
                    .as_ref()
                    .and_then(|c| pump_amm_slave_recovery_snapshot(c, &pool_pk));
                // Pre-plan discovery may have consumed most of the initial pre-send TTL; renew
                // before the second discovery+SLAVE wait so cleanup_expired cannot release capital.
                ctx.lock_manager.renew_capital_lock_ttl(
                    &intent.intent_id,
                    compute_pre_send_capital_lock_ttl(&config),
                );
                if let DiscoveryRequestOutcome::Ok = ctx
                    .request_discovery_and_wait(
                        &intent.resources.input_mint,
                        Some(&pool_pk.to_string()),
                        true,
                    )
                    .await
                {
                    if let Some(cache) = ctx.live_pool_cache.as_ref() {
                        if wait_for_pump_amm_slave_after_recovery(
                            cache,
                            &pool_pk,
                            before_snap,
                            PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                            DISCOVERY_CACHE_POLL_INTERVAL_MS,
                        )
                        .await
                        {
                            let sell_requires_pre_fee_metas =
                                cache.pump_amm_sell_requires_pre_fee_metas(&pool_pk);
                            let sell_requires_fee_tail =
                                cache.pump_amm_sell_requires_fee_tail(&pool_pk);
                            let (sell_requires_extended, _, _, _) =
                                cache.pump_amm_sell_extended_layout(&pool_pk);
                            let sell_ix_account_count =
                                ironcrab::solana::dex::pumpfun_amm::pump_amm_inferred_sell_ix_account_count(
                                    sell_requires_pre_fee_metas,
                                    sell_requires_fee_tail,
                                    sell_requires_extended,
                                );
                            let before_pre_fee = before_snap.map(|s| s.9).unwrap_or(false);
                            if before_pre_fee && !sell_requires_pre_fee_metas {
                                warn!(
                                    intent_id = %intent.intent_id,
                                    pool = %pool_pk,
                                    sim_error = ?sim_result.error_code,
                                    sell_requires_pre_fee_metas,
                                    sell_ix_account_count,
                                    "PumpSwap cold-path recovery: refusing retry — sell_requires_pre_fee_metas degraded from true to false (no 26-account fallback)"
                                );
                                break (
                                    tx_plan,
                                    plan_hash_str,
                                    sim_result,
                                    requires_bundle,
                                    bundle_tip_ix,
                                    wallet_pubkey,
                                    bundle_tip_lamports,
                                );
                            }
                            if before_pre_fee
                                && sell_ix_account_count
                                    < ironcrab::solana::dex::pumpfun_amm::PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS
                                        as u8
                            {
                                warn!(
                                    intent_id = %intent.intent_id,
                                    pool = %pool_pk,
                                    sim_error = ?sim_result.error_code,
                                    sell_requires_pre_fee_metas,
                                    sell_ix_account_count,
                                    expected = ironcrab::solana::dex::pumpfun_amm::PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS,
                                    "PumpSwap cold-path recovery: refusing retry — inferred SELL layout below 27-account SSOT"
                                );
                                break (
                                    tx_plan,
                                    plan_hash_str,
                                    sim_result,
                                    requires_bundle,
                                    bundle_tip_ix,
                                    wallet_pubkey,
                                    bundle_tip_lamports,
                                );
                            }
                            pump_amm_recovery_attempted = true;
                            warn!(
                                intent_id = %intent.intent_id,
                                pool = %pool_pk,
                                sim_error = ?sim_result.error_code,
                                sell_requires_pre_fee_metas,
                                sell_pre_fee_meta_1 = ?cache.pump_amm_sell_pre_fee_meta_1(&pool_pk),
                                sell_ix_account_count,
                                sell_layout_ready = cache.pump_amm_sell_layout_ready(&pool_pk),
                                "PumpSwap cold-path recovery: simulation failed — force-refresh pool_accounts (market-data RPC), rebuilding tx (one retry)"
                            );
                            continue;
                        } else {
                            warn!(
                                intent_id = %intent.intent_id,
                                pool = %pool_pk,
                                timeout_ms = PUMP_AMM_FORCE_REFRESH_SLAVE_WAIT_TIMEOUT_MS,
                                "PumpSwap cold-path recovery: force-refresh reply was Ok, but SLAVE did not expose fresh explicit-ready pool snapshot before timeout"
                            );
                        }
                    }
                }
            }
        }

        // PumpFun bonding curve SELL: stale virtual/real reserves or cashback layout → 6023/6024/Overflow.
        // Cold-path recovery: EnsurePumpfunBondingCurve with force_refresh (RPC in market-data), bounded JetStream wait, one retry.
        // Liquidation/kill-switch: any sim fail triggers one recovery pass; sell-all-only keeps structural gate.
        if !pumpfun_bonding_recovery_attempted
            && is_cold_path_recovery_sell(&intent)
            && intent.metadata.get("dex").map(|s| s.as_str()) == Some("pumpfun")
            && intent.resources.pools.len() == 1
            && cold_path_dex_sim_failure_triggers_discovery_recovery(
                &intent,
                sim_result.error_code.as_deref(),
                is_pumpfun_bonding_curve_structural_sim_error,
            )
        {
            let base_mint_pk = match Pubkey::from_str(&intent.resources.input_mint) {
                Ok(p) => p,
                Err(_) => {
                    break (
                        tx_plan,
                        plan_hash_str,
                        sim_result,
                        requires_bundle,
                        bundle_tip_ix,
                        wallet_pubkey,
                        bundle_tip_lamports,
                    )
                }
            };
            let pumpfun = match PumpFunDex::new(Arc::clone(&ctx.rpc), None) {
                Ok(d) => d,
                Err(_) => {
                    break (
                        tx_plan,
                        plan_hash_str,
                        sim_result,
                        requires_bundle,
                        bundle_tip_ix,
                        wallet_pubkey,
                        bundle_tip_lamports,
                    )
                }
            };
            let (bonding_curve, _) = pumpfun.derive_bonding_curve(&base_mint_pk);
            let pool_str = intent.resources.pools[0].as_str();
            let pool_ok = Pubkey::from_str(pool_str)
                .map(|p| p == bonding_curve)
                .unwrap_or(false);
            if pool_ok {
                let before_snap = ctx
                    .live_pool_cache
                    .as_ref()
                    .and_then(|c| c.pumpfun_bonding_curve_reserves_snapshot(&bonding_curve));
                if let DiscoveryRequestOutcome::Ok = ctx
                    .request_pumpfun_bonding_recovery_and_wait(&intent.resources.input_mint)
                    .await
                {
                    if let Some(cache) = ctx.live_pool_cache.as_ref() {
                        if wait_for_pumpfun_bonding_cache_refresh(
                            cache,
                            &bonding_curve,
                            before_snap,
                            DISCOVERY_CACHE_WAIT_TIMEOUT_MS,
                            DISCOVERY_CACHE_POLL_INTERVAL_MS,
                            true,
                        )
                        .await
                        {
                            pumpfun_bonding_recovery_attempted = true;
                            warn!(
                                intent_id = %intent.intent_id,
                                mint = %intent.resources.input_mint,
                                bonding_curve = %bonding_curve,
                                sim_error = ?sim_result.error_code,
                                "PumpFun cold-path: bonding-curve sim failed — force_refresh via market-data RPC, one tx rebuild retry"
                            );
                            continue;
                        }
                    }
                }
            }
        }

        // Orca Whirlpool SELL (cold-path recovery slice via `is_cold_path_recovery_sell`): stale tick or vault evidence →
        // structural sim fail. I-24d: one EnsureOrcaWhirlpoolPoolState + bounded JetStream wait + one retry.
        // Liquidation/kill-switch: any sim fail triggers one recovery pass; sell-all-only keeps structural gate.
        if !orca_recovery_attempted
            && is_cold_path_recovery_sell(&intent)
            && intent.metadata.get("dex").map(|s| s.as_str()) == Some("orca")
            && intent.resources.pools.len() == 1
            && cold_path_dex_sim_failure_triggers_discovery_recovery(
                &intent,
                sim_result.error_code.as_deref(),
                is_orca_structural_sim_error,
            )
        {
            if Pubkey::from_str(&intent.resources.input_mint).is_err() {
                break (
                    tx_plan,
                    plan_hash_str,
                    sim_result,
                    requires_bundle,
                    bundle_tip_ix,
                    wallet_pubkey,
                    bundle_tip_lamports,
                );
            }
            let pool_pk = match Pubkey::from_str(intent.resources.pools[0].as_str()) {
                Ok(p) => p,
                Err(_) => {
                    break (
                        tx_plan,
                        plan_hash_str,
                        sim_result,
                        requires_bundle,
                        bundle_tip_ix,
                        wallet_pubkey,
                        bundle_tip_lamports,
                    )
                }
            };
            let before_evidence = ctx
                .live_pool_cache
                .as_ref()
                .and_then(|c| c.get(&pool_pk))
                .and_then(|st| match st {
                    CachedPoolState::Orca(s) => Some((
                        s.tick_current_index,
                        s.sqrt_price,
                        s.liquidity,
                        s.vault_a_balance.unwrap_or(0),
                        s.vault_b_balance.unwrap_or(0),
                    )),
                    _ => None,
                });
            warn!(
                intent_id = %intent.intent_id,
                mint = %intent.resources.input_mint,
                pool = %pool_pk,
                sim_error = ?sim_result.error_code,
                before_evidence = ?before_evidence,
                "Orca cold-path: simulation failed — requesting EnsureOrcaWhirlpoolPoolState from market-data (bounded wait + one tx rebuild retry)"
            );
            if let DiscoveryRequestOutcome::Ok = ctx
                .request_orca_whirlpool_recovery_and_wait(
                    &intent.resources.input_mint,
                    Some(&pool_pk.to_string()),
                )
                .await
            {
                if let Some(cache) = ctx.live_pool_cache.as_ref() {
                    if wait_for_orca_whirlpool_slave_after_recovery(
                        cache,
                        &pool_pk,
                        before_evidence,
                        DISCOVERY_CACHE_WAIT_TIMEOUT_MS,
                        DISCOVERY_CACHE_POLL_INTERVAL_MS,
                    )
                    .await
                    {
                        orca_recovery_attempted = true;
                        warn!(
                            intent_id = %intent.intent_id,
                            mint = %intent.resources.input_mint,
                            pool = %pool_pk,
                            "Orca cold-path: SLAVE shows fresh explicit Ready after recovery — rebuilding tx (one retry)"
                        );
                        continue;
                    }
                    warn!(
                        intent_id = %intent.intent_id,
                        pool = %pool_pk,
                        timeout_ms = DISCOVERY_CACHE_WAIT_TIMEOUT_MS,
                        "Orca cold-path: ControlResponse ok but bounded wait for SLAVE explicit Ready + fresh evidence timed out"
                    );
                }
            }
        }

        break (
            tx_plan,
            plan_hash_str,
            sim_result,
            requires_bundle,
            bundle_tip_ix,
            wallet_pubkey,
            bundle_tip_lamports,
        );
    };

    let plan_hash: Option<String> = Some(plan_hash_str.clone());

    if !sim_result.success {
        let reason = sim_failure_reject_reason(sim_result.error_code.as_deref());
        checks.push(CheckResult {
            check_name: "simulation".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: sim_result.error_code.clone(),
        });
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

    if sim_result.success {
        checks.push(CheckResult {
            check_name: "simulation".to_string(),
            passed: true,
            reason_code: None,
            details: Some(format!(
                "CU consumed: {:?}",
                sim_result.compute_units_consumed
            )),
        });
    }

    // Track bundle result for decision record
    let mut bundle_id: Option<String> = None;
    let mut send_signature: Option<String> = None;
    let mut send_method_used: Option<String> = None;
    let mut sent_tx: Option<VersionedTransaction> = None;
    let mut sent_anything = false;
    let mut send_failed = false;
    let mut slot_at_send: Option<u64> = None;
    let mut rebroadcast_count: u32 = 0;

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

            let signer = treasury.signer_ref();
            let blockhash_for_send = match ctx.get_latest_blockhash_for_send().await {
                Ok(bh) => bh,
                Err(e) => {
                    let reason = RejectReason::BundleFailed;
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(format!("blockhash_error:{e}")),
                    });
                    ctx.record_intent_rejected();
                    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!(intent_id = %intent.intent_id, error = %e, "Failed to get blockhash for bundle send");
                    // Allow retry
                    ctx.lock_manager.release_locks(&intent.intent_id);
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }
            };
            let blockhash = blockhash_for_send.hash;
            let bundle_send_slot = blockhash_for_send.slot;

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
                    // K Phase 1: Slot-to-Send Latency
                    if let Some(seen) = intent
                        .metadata
                        .get("slot_seen_at_ms")
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        record_tx_slot_to_send_ms(now_ms.saturating_sub(seen));
                    }
                    record_execution_slot_lag_at_send_if_applicable(ctx, &intent);
                    slot_at_send = Some(bundle_send_slot);
                    record_tx_priority_fee_source(priority_fee_selection.source);
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: true,
                        reason_code: None,
                        details: Some(format!(
                            "bundle_id={bid} tip_lamports={}",
                            bundle_tip_lamports.unwrap_or(config.jito_tip_lamports)
                        )),
                    });
                    info!(
                        intent_id = %intent.intent_id,
                        bundle_id = %bid,
                        priority_fee_micro_lamports = priority_fee_selection.fee_micro_lamports,
                        source = priority_fee_selection.source,
                        slot_at_send = ?slot_at_send,
                        "Bundle submitted via Jito"
                    );
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
                    // K Phase 1: Slot-to-Send Latency
                    if let Some(seen) = intent
                        .metadata
                        .get("slot_seen_at_ms")
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        record_tx_slot_to_send_ms(now_ms.saturating_sub(seen));
                    }
                    record_execution_slot_lag_at_send_if_applicable(ctx, &intent);
                    slot_at_send = Some(result.slot_at_send);
                    record_tx_priority_fee_source(priority_fee_selection.source);
                    send_signature = Some(result.signature.clone());
                    send_method_used = Some(result.method.clone());
                    sent_tx = Some(result.vtx);
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
                        priority_fee_micro_lamports = priority_fee_selection.fee_micro_lamports,
                        source = priority_fee_selection.source,
                        slot_at_send = ?slot_at_send,
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

    // After successful send: capital reservation stays (I-20) but is immune to pre-send TTL;
    // pool resource locks are released (no second TX build on this intent).
    if config.send_enabled
        && sent_anything
        && ctx
            .lock_manager
            .promote_capital_lock_to_in_flight(&intent.intent_id)
    {
        IN_FLIGHT_CAPITAL_RESERVATIONS.store(
            ctx.lock_manager.in_flight_reservation_count() as u64,
            Ordering::Relaxed,
        );
    }

    // Register JetStream confirm waiter before any async pre-confirm work so the main loop
    // cannot ack WalletTxConfirmed while this intent task is still between send and confirm.
    let mut wallet_tx_confirm_rx: Option<tokio::sync::oneshot::Receiver<WalletTxConfirmNotify>> =
        None;
    if config.send_enabled && sent_anything && !requires_bundle {
        if let Some(ref sig) = send_signature {
            wallet_tx_confirm_rx = Some(register_wallet_tx_confirm_waiter(ctx, sig));
        }
    }

    // PR1 Phase 2: pre-confirm ATA track — publish Sent ExecutionResult before confirm wait.
    if config.send_enabled && sent_anything {
        if let Some(ref sr) = send_result {
            if let Err(e) =
                publish_pre_confirm_execution_result(ctx, &intent, &decision_id, sr).await
            {
                warn!(
                    intent_id = %intent.intent_id,
                    error = %e,
                    "Failed to publish pre-confirm ExecutionResult (ATA pre-track)"
                );
            }
        }
    }

    // === Confirm (RS-4.2 / RS-7.4) ===
    // - RPC path (non-bundle): confirm signature via getSignatureStatuses.
    // - Bundle path: wait for bundle landing via Jito block engine.
    let mut final_outcome = if config.send_enabled && sent_anything {
        DecisionOutcome::Sent
    } else {
        DecisionOutcome::Rejected
    };
    // Slot of the confirmed transaction when known (bundle status, Geyser confirm, or RPC status).
    let mut tx_landing_slot: Option<u64> = None;

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
                            tx_landing_slot = Some(status.slot);
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
                match confirm_signature_status(
                    ctx,
                    sig_str,
                    config.confirmation_timeout_ms,
                    sent_tx.as_ref(),
                    wallet_tx_confirm_rx.take(),
                    slot_at_send,
                )
                .await
                {
                    Ok((ConfirmOutcome::Confirmed { details, slot }, rb_count)) => {
                        tx_landing_slot = slot;
                        rebroadcast_count = rb_count;
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: true,
                            reason_code: None,
                            details: Some(details),
                        });
                        if rb_count > 0 {
                            checks.push(CheckResult {
                                check_name: "rebroadcast".to_string(),
                                passed: true,
                                reason_code: None,
                                details: Some(format!("count={rb_count}")),
                            });
                        }
                        final_outcome = DecisionOutcome::Confirmed;
                    }
                    Ok((ConfirmOutcome::FailedConfirmed { details, slot }, rb_count)) => {
                        tx_landing_slot = slot;
                        rebroadcast_count = rb_count;
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: false,
                            reason_code: Some("confirmed_err".to_string()),
                            details: Some(details),
                        });
                        if rb_count > 0 {
                            checks.push(CheckResult {
                                check_name: "rebroadcast".to_string(),
                                passed: true,
                                reason_code: None,
                                details: Some(format!("count={rb_count}")),
                            });
                        }
                        final_outcome = DecisionOutcome::FailedConfirmed;
                    }
                    Ok((ConfirmOutcome::TimeoutSent { details }, rb_count)) => {
                        rebroadcast_count = rb_count;
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: false,
                            reason_code: Some("confirm_timeout".to_string()),
                            details: Some(details),
                        });
                        if rb_count > 0 {
                            checks.push(CheckResult {
                                check_name: "rebroadcast".to_string(),
                                passed: true,
                                reason_code: None,
                                details: Some(format!("count={rb_count}")),
                            });
                        }
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
    if let Some(s) = slot_at_send {
        input_snapshots.insert("slot_at_send".to_string(), s.to_string());
    }
    if let (Some(send_slot), Some(conf_slot)) = (slot_at_send, tx_landing_slot) {
        let delta = confirmed_slot_delta_slots(send_slot, conf_slot);
        input_snapshots.insert("confirmed_slot_delta".to_string(), delta.to_string());
    }
    if sent_anything {
        input_snapshots.insert(
            "priority_fee_source".to_string(),
            priority_fee_selection.source.to_string(),
        );
    }

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
    // Best-effort fills from confirmed TX meta — computed at most once per signature.
    let mut intent_fills_cache: Option<ComputedIntentFills> = None;
    // Capture fill_out for immediate LockManager update after confirmed BUY.
    // Populated inside the should_emit block when fills are available.
    let mut confirmed_buy_fill_out_raw: Option<u64> = None;
    // Confirmed SELL: token `fill_in` raw (sold amount), for LockManager + ExecutionResult metadata.
    let mut confirmed_sell_fill_in_raw: Option<u64> = None;
    let mut confirmed_sell_fill_status: Option<FillStatus> = None;
    // From tx meta only: wallet had input mint pre-tx and zero post-tx token balance rows.
    let mut confirmed_sell_wallet_balance_absent_after_tx: bool = false;
    let mut confirmed_block_time_unix_ms: Option<u64> = None;

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
            )
            .with_metadata(intent.metadata.clone());

            // Always include the trade side in execution result metadata so downstream
            // consumers (trades_server, Grafana) can reliably determine BUY/SELL
            // without heuristics based on wallet_sol_delta_lamports.
            exec.metadata.insert(
                "side".to_string(),
                match intent.side {
                    TradeSide::Buy => "BUY".to_string(),
                    TradeSide::Sell => "SELL".to_string(),
                },
            );

            enrich_execution_result_ata_metadata(&mut exec, ctx, &intent);
            if let Some(token_account) = exec.metadata.get("token_account") {
                info!(
                    intent_id = %intent.intent_id,
                    token_account = %token_account,
                    side = ?exec.metadata.get("side"),
                    "ExecutionResult: enriched metadata for market-data ATA tracking"
                );
            }

            // Best-effort fill accounting: attach fills only when we have a signature and wallet.
            // This is used downstream for correct position accounting/exit sizing.
            if matches!(status, ExecutionStatus::Confirmed) {
                if let (Some(wallet), Some(sig_str)) =
                    (ctx.wallet_pubkey, exec.signature.as_deref())
                {
                    if let Ok(sig) = Signature::from_str(sig_str) {
                        if intent_fills_cache.is_none() {
                            intent_fills_cache = Some(
                                compute_intent_fills_best_effort(ctx, wallet, &sig, &intent).await,
                            );
                        }
                        let fills = intent_fills_cache.as_ref().expect("fills cached");

                        // Capture fill_out for immediate LockManager update (Fix: SIM_INSUFFICIENT_BALANCE)
                        if intent.side == TradeSide::Buy {
                            if let Some(ref fo) = fills.fill_out {
                                confirmed_buy_fill_out_raw = Some(fo.raw);
                            }
                        } else if intent.side == TradeSide::Sell {
                            confirmed_sell_fill_status = Some(fills.fill_status);
                            confirmed_sell_wallet_balance_absent_after_tx =
                                fills.wallet_token_balance_absent_after_tx;
                            if let Some(ref fi) = fills.fill_in {
                                confirmed_sell_fill_in_raw = Some(fi.raw);
                            }
                        }

                        exec = exec
                            .with_fills(fills.fill_in.clone(), fills.fill_out.clone())
                            .with_fill_diagnostics(
                                fills.fill_status,
                                fills.fill_unavailable_reason,
                            );
                        if let Some(delta) = fills.wallet_sol_delta {
                            exec = exec.with_sol_delta(delta);
                        }
                        if let Some(bt_ms) = fills.block_time_unix_ms {
                            exec.block_time_unix_ms = Some(bt_ms);
                            confirmed_block_time_unix_ms = Some(bt_ms);
                        }
                    }
                }
            }

            exec.status = status;
            if exec.status == ExecutionStatus::Confirmed {
                try_record_execution_intent_to_confirm_ms(
                    wall_clock_unix_ms_now(),
                    intent.header.ts_unix_ms,
                );
                if let Some(slot) = tx_landing_slot {
                    exec.confirmed_slot = Some(slot);
                    exec.metadata
                        .insert("confirmed_slot".to_string(), slot.to_string());
                }
                if let Some(s) = slot_at_send {
                    exec.metadata
                        .insert("slot_at_send".to_string(), s.to_string());
                }
                if let (Some(send_slot), Some(conf_slot)) = (slot_at_send, tx_landing_slot) {
                    exec.metadata.insert(
                        "confirmed_slot_delta".to_string(),
                        confirmed_slot_delta_slots(send_slot, conf_slot).to_string(),
                    );
                }
                exec.metadata.insert(
                    "priority_fee_source".to_string(),
                    priority_fee_selection.source.to_string(),
                );
                if rebroadcast_count > 0 {
                    exec.metadata.insert(
                        "rebroadcast_count".to_string(),
                        rebroadcast_count.to_string(),
                    );
                }
            }
            // Scope 48: always classify confirmed SELL for market-data, even when fill RPC path
            // was skipped (no signature / parse failure / missing wallet).
            if intent.side == TradeSide::Sell && exec.status == ExecutionStatus::Confirmed {
                apply_scope48_confirmed_sell_execution_metadata(
                    &mut exec,
                    &ctx.lock_manager,
                    &intent,
                    confirmed_sell_fill_in_raw,
                    confirmed_sell_wallet_balance_absent_after_tx,
                );
            }
            if matches!(exec.status, ExecutionStatus::Failed) {
                exec.error_message = Some("execution_failed".to_string());
                exec.error_code = decision
                    .simulate
                    .as_ref()
                    .and_then(|s| s.error_code.clone());
            }

            ctx.execution_writer.write(&exec)?;
            if let Some(ref nats) = ctx.nats {
                nats.jetstream_publish(TOPIC_EXECUTION_RESULTS, &exec)
                    .await?;
            }
            if exec.status == ExecutionStatus::Confirmed {
                ctx.apply_position_authority_from_execution_result(&exec);
                if intent.side == TradeSide::Buy {
                    ctx.publish_open_position_pool_pin_after_confirmed_buy(&intent, &exec)
                        .await;
                }
            }
        }
    }

    // Dashboard alignment: count executions when we have an on-chain outcome (success or failed).
    // FailedConfirmed = TX was sent and confirmed on-chain but failed (e.g. slippage).
    if matches!(
        decision.outcome,
        DecisionOutcome::Confirmed | DecisionOutcome::FailedConfirmed
    ) {
        INTENTS_EXECUTED_TOTAL.fetch_add(1, Ordering::Relaxed);

        // Primary `open_positions` gauge is refreshed from PositionAuthority (PA-2 Rest).
        // LockManager balance updates below affect lockmanager overlay gauge only.
        match intent.side {
            TradeSide::Buy => {
                // Immediately update LockManager with bought token balance so that
                // subsequent SELL intents don't fail with SIM_INSUFFICIENT_BALANCE.
                //
                // Strategy: Use ADD only when the current Geyser-sourced balance
                // hasn't already accounted for this fill. The live JetStream consumer
                // sets the authoritative balance via `set_available_token_balance`.
                // If Geyser delivered the update BEFORE this handler runs, the current
                // balance already includes this fill — adding would double-count.
                //
                // Detection: add the fill, then cap at max(current_before + fill, current_after_geyser).
                // Practically: only add if current balance < expected accumulated total.
                if let Some(fill_raw) = confirmed_buy_fill_out_raw {
                    let mint_str = &intent.resources.output_mint;
                    let current_before = ctx.lock_manager.available_token_balance(mint_str);

                    // Heuristic: if the current balance is already >= the fill amount
                    // AND we know Geyser has been active (balance > 0 before this fill),
                    // then Geyser likely already includes this fill. Use max() to avoid
                    // double-counting while still ensuring the balance is at least fill_raw.
                    if current_before >= fill_raw && current_before > 0 {
                        // Geyser likely already delivered the authoritative balance.
                        // Don't add — just ensure it's at least fill_raw (defensive).
                        info!(
                            intent_id = %intent.intent_id,
                            mint = %mint_str,
                            fill_out_raw = fill_raw,
                            current_balance = current_before,
                            "LockManager: Geyser balance already >= fill, skipping add to prevent double-count"
                        );
                    } else {
                        // Geyser hasn't caught up yet, or this is the first BUY.
                        // Add the fill to bridge until Geyser delivers.
                        ctx.lock_manager
                            .add_available_token_balance(mint_str.to_string(), fill_raw);
                        let new_total = ctx.lock_manager.available_token_balance(mint_str);
                        info!(
                            intent_id = %intent.intent_id,
                            mint = %mint_str,
                            fill_out_raw = fill_raw,
                            balance_before = current_before,
                            total_available = new_total,
                            "LockManager: accumulated token balance from confirmed BUY fill"
                        );
                    }
                } else {
                    warn!(
                        intent_id = %intent.intent_id,
                        mint = %intent.resources.output_mint,
                        "LockManager: no fill_out available after confirmed BUY — \
                         token balance NOT seeded (will rely on Geyser pipeline)"
                    );
                }
            }
            TradeSide::Sell => {
                // Amount-aware SELL (Scope 48): partial sells must not zero the mint or
                // double-subtract when `release_locks_after_confirmed_sell` runs (lock still holds sold amount).
                let mint_str = intent.resources.input_mint.clone();
                let (avail, locked) = ctx
                    .lock_manager
                    .available_and_locked_tokens_for_intent(&intent.intent_id, &mint_str);
                let total_pos = ctx
                    .lock_manager
                    .intent_token_position_total_at_lock(&intent.intent_id, &mint_str)
                    .unwrap_or_else(|| avail.saturating_add(locked));
                let sold_raw = confirmed_sell_fill_in_raw.unwrap_or(intent.required_capital.raw);
                let is_cold_path_recovery = is_cold_path_recovery_sell(&intent);
                let s48 = scope48_confirmed_sell_close_decision(
                    is_cold_path_recovery,
                    sold_raw,
                    total_pos,
                    confirmed_sell_wallet_balance_absent_after_tx,
                );

                if s48.full_close {
                    ctx.lock_manager
                        .set_available_token_balance(mint_str.clone(), 0);
                    info!(
                        intent_id = %intent.intent_id,
                        mint = %mint_str,
                        sold_raw,
                        total_pos,
                        is_cold_path_recovery,
                        sell_token_account_closed = s48.sell_token_account_closed,
                        "LockManager: cleared token balance after confirmed full SELL"
                    );
                } else {
                    // Partial SELL: `available_tokens` already reflects post-sell unlocked balance
                    // (total minus lock). Do not `set(..., 0)` or subtract — would double-count vs
                    // `release_locks_after_confirmed_sell` (Scope 47 + Scope 48).
                    info!(
                        intent_id = %intent.intent_id,
                        mint = %mint_str,
                        sold_raw,
                        total_pos,
                        fill_status = ?confirmed_sell_fill_status,
                        "LockManager: partial confirmed SELL — leaving token balance unchanged until lock release"
                    );
                }
            }
        }

        // Best-effort recent trade record for Grafana (/trades via Infinity datasource).
        // NOTE: If fill accounting is available, use it; otherwise fall back to placeholders.
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;

        let tx_hash = send_result
            .as_ref()
            .and_then(|sr| sr.signature.clone())
            .or_else(|| send_signature.clone())
            .unwrap_or_default();

        if intent_fills_cache.is_none() {
            if let (Some(wallet), Ok(sig)) = (ctx.wallet_pubkey, Signature::from_str(&tx_hash)) {
                intent_fills_cache =
                    Some(compute_intent_fills_best_effort(ctx, wallet, &sig, &intent).await);
            }
        }
        let (fill_in, fill_out, _fill_status, _fill_reason, _wallet_sol_delta) =
            if let Some(ref f) = intent_fills_cache {
                if confirmed_block_time_unix_ms.is_none() {
                    confirmed_block_time_unix_ms = f.block_time_unix_ms;
                }
                (
                    f.fill_in.clone(),
                    f.fill_out.clone(),
                    f.fill_status,
                    f.fill_unavailable_reason,
                    f.wallet_sol_delta,
                )
            } else {
                (None, None, FillStatus::Unavailable, None, None)
            };

        let (mint, action, amount_tokens, value_sol) = match intent.side {
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
            timestamp_ms: confirmed_block_time_unix_ms.unwrap_or(now_ms),
            block_time_unix_ms: confirmed_block_time_unix_ms,
            mint,
            action,
            tx_hash,
            amount_tokens,
            value_sol,
            pnl_sol: None,
            pnl_pct: None,
            latency_ms: None,
        });
    }

    IN_FLIGHT_CAPITAL_RESERVATIONS.store(
        ctx.lock_manager.in_flight_reservation_count() as u64,
        Ordering::Relaxed,
    );

    // Release lock at terminal outcome (confirmed / failed-confirmed / timeout-sent path).
    if matches!(decision.outcome, DecisionOutcome::Confirmed) && intent.side == TradeSide::Sell {
        // Do not re-add the sold input tokens to `available_tokens` (ghost open_positions).
        ctx.lock_manager
            .release_locks_after_confirmed_sell(&intent.intent_id);
    } else {
        ctx.lock_manager.release_locks(&intent.intent_id);
    }

    if matches!(
        decision.outcome,
        DecisionOutcome::Confirmed | DecisionOutcome::FailedConfirmed
    ) {
        ctx.refresh_position_authority_metrics();
    }

    info!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        outcome = ?decision.outcome,
        "Intent processed"
    );

    Ok(())
}

/// Emit an expired decision record (I-11: `DecisionOutcome::Expired`)
async fn emit_expired_decision(
    ctx: &ExecutionContext,
    decision_id: String,
    intent: &TradeIntent,
    checks: Vec<CheckResult>,
    reason: RejectReason,
) -> Result<()> {
    REJECT_TTL_EXPIRED.fetch_add(1, Ordering::Relaxed);
    INTENTS_EXPIRED_TOTAL.fetch_add(1, Ordering::Relaxed);

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
    decision.outcome = DecisionOutcome::Expired;
    if let Some(sell_routing) = intent.metadata.get("sell_routing") {
        decision = decision.with_input_snapshot("sell_routing".to_string(), sell_routing.clone());
    }
    if let Some(exit_type) = intent.metadata.get("exit_type") {
        decision = decision.with_input_snapshot("exit_type".to_string(), exit_type.clone());
    }
    if let Some(reason_detail) = intent.metadata.get("reason_detail") {
        decision = decision.with_input_snapshot("reason_detail".to_string(), reason_detail.clone());
    }

    ctx.decision_writer.write(&decision)?;

    if let Some(ref nats) = ctx.nats {
        nats.publish(TOPIC_DECISION_RECORDS, &decision).await?;
    }

    ctx.lock_manager.mark_processed(&intent.intent_id);

    warn!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        reason = %reason,
        "Intent expired"
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
    // Capture momentum exit metadata for decision record analysis
    if let Some(exit_type) = intent.metadata.get("exit_type") {
        decision = decision.with_input_snapshot("exit_type".to_string(), exit_type.clone());
    }
    if let Some(reason_detail) = intent.metadata.get("reason_detail") {
        decision = decision.with_input_snapshot("reason_detail".to_string(), reason_detail.clone());
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

    // Extract diagnostic info before sim_result is moved into DecisionRecord
    let sim_error_str = sim_result
        .error_code
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let sim_logs_str = sim_result
        .logs_preview
        .clone()
        .unwrap_or_else(|| "no logs".to_string());

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
    // Capture momentum exit metadata for decision record analysis
    if let Some(exit_type) = intent.metadata.get("exit_type") {
        decision = decision.with_input_snapshot("exit_type".to_string(), exit_type.clone());
    }
    if let Some(reason_detail) = intent.metadata.get("reason_detail") {
        decision = decision.with_input_snapshot("reason_detail".to_string(), reason_detail.clone());
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

    // Emit ExecutionResult so momentum_bot receives SimFailed and can act on 6005/6023
    let token_mint = match intent.side {
        TradeSide::Buy => Some(intent.resources.output_mint.clone()),
        TradeSide::Sell => Some(intent.resources.input_mint.clone()),
    };
    let mut exec = ExecutionResult::new_sent(
        "execution-engine",
        BUILD_VERSION,
        &ctx.run_id,
        ctx.next_execution_id(),
        decision_id.clone(),
        intent.intent_id.clone(),
        intent.source.clone(),
        token_mint,
        None,
        None,
    )
    .with_metadata(intent.metadata.clone())
    .with_error_code(Some(sim_error_str.clone()));
    exec.metadata.insert(
        "side".to_string(),
        match intent.side {
            TradeSide::Buy => "BUY".to_string(),
            TradeSide::Sell => "SELL".to_string(),
        },
    );
    exec.status = ExecutionStatus::Failed;
    exec.error_message = Some("execution_failed".to_string());

    if ctx.execution_writer.write(&exec).is_err() {
        warn!(intent_id = %intent.intent_id, "Failed to write SimFailed ExecutionResult to JSONL");
    }
    if let Some(ref nats) = ctx.nats {
        if let Err(e) = nats.jetstream_publish(TOPIC_EXECUTION_RESULTS, &exec).await {
            warn!(error = %e, intent_id = %intent.intent_id, "Failed to publish SimFailed ExecutionResult");
        }
    }

    // Enhanced simulation failure logging (Fix 2: diagnostic info for debugging)
    let logs_truncated = if sim_logs_str.len() > 500 {
        &sim_logs_str[..500]
    } else {
        &sim_logs_str
    };

    warn!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        dex = intent.metadata.get("dex").map(|s| s.as_str()).unwrap_or("?"),
        side = ?intent.side,
        mint = intent.metadata.get("token_mint").or_else(|| intent.metadata.get("mint")).map(|s| s.as_str()).unwrap_or("?"),
        error = %sim_error_str,
        "Intent simulation failed | logs: {logs_truncated}"
    );

    // Return Err so callers (e.g. liquidation 6005-retry) can detect and act on sim failures.
    Err(anyhow::anyhow!("Simulation failed: {}", sim_error_str))
}

/// Solana wire-format size limit for serialized transactions.
const MAX_SERIALIZED_TX_BYTES: usize = 1232;

/// Build the versioned message shared by simulation and send.
///
/// v0+ALT when an ALT is configured, otherwise legacy.
fn build_versioned_message(
    wallet_pubkey: &Pubkey,
    plan: &tx_builder::TxPlan,
    blockhash: Hash,
    alt: Option<&ironcrab::solana::address_lookup_table::LoadedAlt>,
) -> Result<VersionedMessage, String> {
    if let Some(alt) = alt {
        let alt_account = AddressLookupTableAccount {
            key: alt.address,
            addresses: alt.accounts.clone(),
        };
        v0::Message::try_compile(wallet_pubkey, &plan.instructions, &[alt_account], blockhash)
            .map(VersionedMessage::V0)
            .map_err(|e| format!("v0_compile_error:{e}"))
    } else {
        let message = solana_sdk::message::Message::new_with_blockhash(
            &plan.instructions,
            Some(wallet_pubkey),
            &blockhash,
        );
        Ok(VersionedMessage::Legacy(message))
    }
}

/// Unsigned versioned transaction for RPC simulation (`sig_verify=false`).
fn build_unsigned_versioned_tx(
    wallet_pubkey: &Pubkey,
    plan: &tx_builder::TxPlan,
    blockhash: Hash,
    alt: Option<&ironcrab::solana::address_lookup_table::LoadedAlt>,
) -> Result<VersionedTransaction, String> {
    let message = build_versioned_message(wallet_pubkey, plan, blockhash, alt)?;
    Ok(VersionedTransaction {
        signatures: vec![Signature::default()],
        message,
    })
}

/// Signed versioned transaction for send and rebroadcast.
fn build_signed_versioned_tx(
    wallet_pubkey: &Pubkey,
    plan: &tx_builder::TxPlan,
    blockhash: Hash,
    alt: Option<&ironcrab::solana::address_lookup_table::LoadedAlt>,
    signers: &[&dyn Signer],
) -> Result<VersionedTransaction, String> {
    let message = build_versioned_message(wallet_pubkey, plan, blockhash, alt)?;
    VersionedTransaction::try_new(message, signers).map_err(|e| format!("sign_error:{e}"))
}

fn versioned_tx_serialized_len(tx: &VersionedTransaction) -> Result<usize, String> {
    bincode::serialize(tx)
        .map(|bytes| bytes.len())
        .map_err(|e| format!("serialize_error:{e}"))
}

fn check_versioned_tx_size(tx: &VersionedTransaction) -> Result<(), String> {
    let size = versioned_tx_serialized_len(tx)?;
    if size > MAX_SERIALIZED_TX_BYTES {
        return Err(format!(
            "tx_too_large:{size}_bytes_max_{MAX_SERIALIZED_TX_BYTES}"
        ));
    }
    Ok(())
}

/// Build + size-gate the unsigned TX used for simulation (same form as send, no RPC yet).
fn prepare_unsigned_versioned_tx_for_simulation(
    wallet_pubkey: &Pubkey,
    plan: &tx_builder::TxPlan,
    blockhash: Hash,
    alt: Option<&ironcrab::solana::address_lookup_table::LoadedAlt>,
) -> Result<VersionedTransaction, String> {
    let tx = build_unsigned_versioned_tx(wallet_pubkey, plan, blockhash, alt)?;
    check_versioned_tx_size(&tx)?;
    Ok(tx)
}

/// Real RPC simulation (RS-3.1).
///
/// Notes:
/// - Uses `sig_verify=false` (unsigned tx is fine for simulation).
/// - Uses `replace_recent_blockhash=true` so simulation does not depend on blockhash freshness.
/// - Uses the same message form as send (v0+ALT or legacy).
async fn simulate_transaction(
    ctx: &ExecutionContext,
    wallet_pubkey: Pubkey,
    plan: &tx_builder::TxPlan,
) -> SimulationResult {
    let blockhash = match ctx.get_latest_blockhash().await {
        Ok(hash) => hash,
        Err(e) => {
            return SimulationResult {
                success: false,
                error_code: Some(format!("blockhash_error:{e}")),
                logs_preview: None,
                compute_units_consumed: None,
            };
        }
    };

    let tx_result = prepare_unsigned_versioned_tx_for_simulation(
        &wallet_pubkey,
        plan,
        blockhash,
        ctx.address_lookup_table.as_ref(),
    );

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

    if let Err(e) = check_versioned_tx_size(&tx) {
        return SimulationResult {
            success: false,
            error_code: Some(e),
            logs_preview: None,
            compute_units_consumed: None,
        };
    }

    let cfg = RpcSimulateTransactionConfig {
        sig_verify: false,
        replace_recent_blockhash: true,
        commitment: Some(CommitmentConfig::processed()),
        ..RpcSimulateTransactionConfig::default()
    };

    let timeout_ms = ctx.get_config().simulation_timeout_ms;
    let rpc_future = ctx.rpc.rpc.simulate_transaction_with_config(&tx, cfg);
    match tokio::time::timeout(Duration::from_millis(timeout_ms), rpc_future).await {
        Ok(Ok(res)) => {
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
        Ok(Err(e)) => SimulationResult {
            success: false,
            error_code: Some(format!("rpc_error:{e}")),
            logs_preview: None,
            compute_units_consumed: None,
        },
        Err(_elapsed) => {
            SIM_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
            simulation_result_on_rpc_timeout()
        }
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
    TX_SEND_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    let treasury = ctx
        .treasury
        .as_ref()
        .ok_or_else(|| "no_signer_configured".to_string())?;

    let signer = treasury.signer_ref();
    let blockhash = ctx.get_latest_blockhash().await?;

    let tx = build_signed_versioned_tx(
        &wallet_pubkey,
        plan,
        blockhash,
        ctx.address_lookup_table.as_ref(),
        &[signer],
    )?;
    check_versioned_tx_size(&tx)?;

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
    method: String,            // "tpu", "jito", "rpc"
    vtx: VersionedTransaction, // exact signed TX for rebroadcast during confirm
    slot_at_send: u64,
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

    let signer = treasury.signer_ref();
    let blockhash_for_send = ctx.get_latest_blockhash_for_send().await?;

    // Same message form as simulation: v0+ALT when configured, else legacy.
    let vtx = build_signed_versioned_tx(
        &wallet_pubkey,
        plan,
        blockhash_for_send.hash,
        ctx.address_lookup_table.as_ref(),
        &[signer],
    )?;
    check_versioned_tx_size(&vtx)?;

    // If TxSender is available, use it for the fallback chain
    if let Some(ref tx_sender) = ctx.tx_sender {
        match tx_sender
            .send_versioned_with_fallback(&vtx, require_bundle)
            .await
        {
            Ok(result) => {
                let method_str = result.method.to_string();
                info!(
                    signature = %result.signature,
                    method = %method_str,
                    bundle_id = ?result.bundle_id,
                    slot_at_send = blockhash_for_send.slot,
                    "TX sent via TxSender"
                );
                return Ok(SendTxResult {
                    signature: result.signature.to_string(),
                    method: method_str,
                    vtx,
                    slot_at_send: blockhash_for_send.slot,
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

    // IMPORTANT:
    // Reuse the *exact* signed transaction we are sending as `sent_tx` for any rebroadcasts
    // during confirmation polling. Creating a new Transaction here could change the signature
    // (e.g. if blockhash changes or signing is not perfectly deterministic).
    match ctx.rpc.rpc.send_transaction_with_config(&vtx, config).await {
        Ok(sig) => Ok(SendTxResult {
            signature: sig.to_string(),
            method: "rpc".into(),
            vtx,
            slot_at_send: blockhash_for_send.slot,
        }),
        Err(e) => Err(format!("rpc_error:{e}")),
    }
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
    // Capture momentum exit metadata for decision record analysis
    if let Some(exit_type) = intent.metadata.get("exit_type") {
        snapshots.insert("exit_type".to_string(), exit_type.clone());
    }
    if let Some(reason_detail) = intent.metadata.get("reason_detail") {
        snapshots.insert("reason_detail".to_string(), reason_detail.clone());
    }
    snapshots
}

/// Payload delivered when a pending TX receives `WalletTxConfirmed` from JetStream.
#[derive(Debug, Clone)]
struct WalletTxConfirmNotify {
    slot: u64,
    err: Option<String>,
}

/// PR3.1: Orphan confirm buffered before `register_wallet_tx_confirm_waiter` (main-loop race).
#[derive(Debug, Clone)]
struct OrphanTxConfirmEntry {
    notify: WalletTxConfirmNotify,
    buffered_at: std::time::Instant,
}

/// Default TTL for orphan confirms — longer than typical `confirmation_timeout_ms` (30s).
const ORPHAN_TX_CONFIRM_TTL: std::time::Duration = std::time::Duration::from_secs(120);

fn register_wallet_tx_confirm_waiter(
    ctx: &ExecutionContext,
    signature_base58: &str,
) -> tokio::sync::oneshot::Receiver<WalletTxConfirmNotify> {
    // PR3.1: Confirm may have arrived on the 100ms poll before post-send registration.
    if let Some(orphan) = ctx
        .recent_orphan_tx_confirms
        .write()
        .remove(signature_base58)
    {
        let slot = orphan.notify.slot;
        let (notify_tx, notify_rx) = tokio::sync::oneshot::channel();
        let _ = notify_tx.send(orphan.notify);
        TX_CONFIRM_JETSTREAM_ORPHAN_HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
        info!(
            sig = %signature_base58,
            slot,
            "WalletTxConfirmed orphan buffer hit (confirm arrived before waiter registration)"
        );
        return notify_rx;
    }

    let (notify_tx, notify_rx) = tokio::sync::oneshot::channel();
    ctx.pending_tx_confirms
        .write()
        .insert(signature_base58.to_string(), notify_tx);

    // Second orphan check: dispatch may have buffered between the first check and pending insert.
    if let Some(orphan) = ctx
        .recent_orphan_tx_confirms
        .write()
        .remove(signature_base58)
    {
        if let Some(notify_tx) = ctx.pending_tx_confirms.write().remove(signature_base58) {
            let slot = orphan.notify.slot;
            let _ = notify_tx.send(orphan.notify);
            TX_CONFIRM_JETSTREAM_ORPHAN_HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
            info!(
                sig = %signature_base58,
                slot,
                "WalletTxConfirmed orphan buffer hit (confirm arrived before waiter registration)"
            );
        }
    }

    notify_rx
}

fn dispatch_wallet_tx_confirmed(
    pending: &Arc<
        parking_lot::RwLock<
            std::collections::HashMap<String, tokio::sync::oneshot::Sender<WalletTxConfirmNotify>>,
        >,
    >,
    orphan_buffer: &Arc<
        parking_lot::RwLock<std::collections::HashMap<String, OrphanTxConfirmEntry>>,
    >,
    signature: &str,
    slot: u64,
    err: Option<String>,
) {
    let notify = WalletTxConfirmNotify { slot, err };
    if let Some(notify_tx) = pending.write().remove(signature) {
        let _ = notify_tx.send(notify);
        return;
    }

    // PR3.1: Buffer orphan — main loop can consume confirm before register_wallet_tx_confirm_waiter.
    orphan_buffer.write().insert(
        signature.to_string(),
        OrphanTxConfirmEntry {
            notify,
            buffered_at: std::time::Instant::now(),
        },
    );
    TX_CONFIRM_JETSTREAM_ORPHAN_BUFFERED_TOTAL.fetch_add(1, Ordering::Relaxed);
    debug!(
        sig = %signature,
        slot,
        "WalletTxConfirmed buffered (orphan, no waiter yet)"
    );
}

fn evict_stale_orphan_tx_confirms(
    orphan_buffer: &Arc<
        parking_lot::RwLock<std::collections::HashMap<String, OrphanTxConfirmEntry>>,
    >,
    ttl: std::time::Duration,
) {
    let now = std::time::Instant::now();
    let mut guard = orphan_buffer.write();
    let before = guard.len();
    guard.retain(|_, entry| now.duration_since(entry.buffered_at) < ttl);
    let evicted = before.saturating_sub(guard.len());
    if evicted > 0 {
        TX_CONFIRM_JETSTREAM_ORPHAN_EVICTED_TOTAL.fetch_add(evicted as u64, Ordering::Relaxed);
    }
}

#[derive(Debug)]
enum ConfirmOutcome {
    Confirmed {
        details: String,
        /// Execution landing slot when known (JetStream WalletTxConfirmed or bundle watcher).
        slot: Option<u64>,
    },
    FailedConfirmed {
        details: String,
        slot: Option<u64>,
    },
    TimeoutSent {
        details: String,
    },
}

/// Derive ATA metadata (PDA, no RPC) so market-data can pre-track wallet token accounts.
fn enrich_execution_result_ata_metadata(
    exec: &mut ExecutionResult,
    ctx: &ExecutionContext,
    intent: &TradeIntent,
) {
    if intent.side == TradeSide::Buy {
        if let Some(wallet) = ctx.wallet_pubkey {
            let output_mint_str = &intent.resources.output_mint;
            if let Ok(output_mint_pk) = Pubkey::from_str(output_mint_str) {
                use spl_token::solana_program::pubkey::Pubkey as SplPubkey;

                let token_program_spl = intent
                    .resources
                    .token_program
                    .as_ref()
                    .and_then(|tp| Pubkey::from_str(tp).ok())
                    .map(|pk| SplPubkey::new_from_array(pk.to_bytes()))
                    .unwrap_or_else(spl_token::id);

                let wallet_spl = SplPubkey::new_from_array(wallet.to_bytes());
                let mint_spl = SplPubkey::new_from_array(output_mint_pk.to_bytes());
                let ata_spl =
                    spl_associated_token_account::get_associated_token_address_with_program_id(
                        &wallet_spl,
                        &mint_spl,
                        &token_program_spl,
                    );
                let ata_pk = Pubkey::new_from_array(ata_spl.to_bytes());
                let token_program_pk = Pubkey::new_from_array(token_program_spl.to_bytes());

                exec.metadata
                    .entry("token_account".to_string())
                    .or_insert_with(|| ata_pk.to_string());
                exec.metadata
                    .entry("token_program".to_string())
                    .or_insert_with(|| token_program_pk.to_string());

                if !exec.metadata.contains_key("mint_decimals") {
                    if let Some(ref cache) = ctx.live_pool_cache {
                        if let Some(d) = cache.get_mint_decimals(&output_mint_pk) {
                            exec.metadata
                                .insert("mint_decimals".to_string(), d.to_string());
                        }
                    }
                }
            }
        }
    } else if intent.side == TradeSide::Sell {
        if let Some(wallet) = ctx.wallet_pubkey {
            let input_mint_str = &intent.resources.input_mint;
            if let Ok(input_mint_pk) = Pubkey::from_str(input_mint_str) {
                use spl_token::solana_program::pubkey::Pubkey as SplPubkey;

                let token_program_spl = intent
                    .resources
                    .token_program
                    .as_ref()
                    .and_then(|tp| Pubkey::from_str(tp).ok())
                    .map(|pk| SplPubkey::new_from_array(pk.to_bytes()))
                    .unwrap_or_else(spl_token::id);

                let wallet_spl = SplPubkey::new_from_array(wallet.to_bytes());
                let mint_spl = SplPubkey::new_from_array(input_mint_pk.to_bytes());
                let ata_spl =
                    spl_associated_token_account::get_associated_token_address_with_program_id(
                        &wallet_spl,
                        &mint_spl,
                        &token_program_spl,
                    );
                let ata_pk = Pubkey::new_from_array(ata_spl.to_bytes());
                let token_program_pk = Pubkey::new_from_array(token_program_spl.to_bytes());

                exec.metadata
                    .entry("token_account".to_string())
                    .or_insert_with(|| ata_pk.to_string());
                exec.metadata
                    .entry("token_program".to_string())
                    .or_insert_with(|| token_program_pk.to_string());

                if !exec.metadata.contains_key("mint_decimals") {
                    if let Some(ref cache) = ctx.live_pool_cache {
                        if let Some(d) = cache.get_mint_decimals(&input_mint_pk) {
                            exec.metadata
                                .insert("mint_decimals".to_string(), d.to_string());
                        }
                    }
                }
            }
        }
    }
}

/// PR1 Phase 2: publish `ExecutionStatus::Sent` immediately after send so market-data can
/// pin the ATA before confirm (~30s). Uses a **distinct** `execution_id` from the post-confirm
/// result so `execution_results_deduper` in market-data processes both events (ATA track is idempotent).
async fn publish_pre_confirm_execution_result(
    ctx: &ExecutionContext,
    intent: &TradeIntent,
    decision_id: &str,
    send_result: &ironcrab::ipc::SendResult,
) -> Result<()> {
    let token_mint = match intent.side {
        TradeSide::Buy => Some(intent.resources.output_mint.clone()),
        TradeSide::Sell => Some(intent.resources.input_mint.clone()),
    };

    let mut exec = ExecutionResult::new_sent(
        "execution-engine",
        BUILD_VERSION,
        &ctx.run_id,
        ctx.next_execution_id(),
        decision_id.to_string(),
        intent.intent_id.clone(),
        intent.source.clone(),
        token_mint,
        send_result.signature.clone(),
        send_result.bundle_id.clone(),
    )
    .with_metadata(intent.metadata.clone());

    exec.metadata.insert(
        "side".to_string(),
        match intent.side {
            TradeSide::Buy => "BUY".to_string(),
            TradeSide::Sell => "SELL".to_string(),
        },
    );
    exec.metadata
        .insert("phase".to_string(), "pre_confirm_track".to_string());

    enrich_execution_result_ata_metadata(&mut exec, ctx, intent);

    let token_account = exec.metadata.get("token_account").cloned();
    info!(
        intent_id = %intent.intent_id,
        token_account = ?token_account,
        phase = "pre_confirm_track",
        "Early ExecutionResult published for market-data ATA pre-track"
    );

    ctx.execution_writer.write(&exec)?;
    if let Some(ref nats) = ctx.nats {
        nats.jetstream_publish(TOPIC_EXECUTION_RESULTS, &exec)
            .await?;
    }
    Ok(())
}

/// PR3: JetStream WalletTxConfirmed wait (market-data Geyser → JetStream → EE).
///
/// No EE Geyser client and no RPC fallback on timeout (I-4 / I-7).
/// Rebroadcasts run in a parallel task regardless of confirmation source.
async fn confirm_signature_status(
    ctx: &ExecutionContext,
    signature_base58: &str,
    timeout_ms: u64,
    rebroadcast_tx: Option<&VersionedTransaction>,
    pre_registered_rx: Option<tokio::sync::oneshot::Receiver<WalletTxConfirmNotify>>,
    slot_at_send: Option<u64>,
) -> std::result::Result<(ConfirmOutcome, u32), String> {
    let start = std::time::Instant::now();
    let deadline = Duration::from_millis(timeout_ms.max(1));
    let config = ctx.config.read().clone();
    let rebroadcast_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

    // Spawn rebroadcast task (runs in parallel for both strategies)
    let rebroadcast_handle = if let Some(tx) = rebroadcast_tx {
        let rpc = Arc::clone(&ctx.rpc);
        let tx_sender = ctx.tx_sender.clone();
        let sig_str = signature_base58.to_string();
        let tx_clone = tx.clone();
        let interval_ms = config.rebroadcast_interval_ms;
        let max_rebroadcasts = config.max_rebroadcasts;
        let rebroadcast_use_tpu = config.rebroadcast_use_tpu;
        let rb_count = Arc::clone(&rebroadcast_count);
        Some(tokio::spawn(async move {
            spawn_rebroadcast_loop(
                rpc,
                tx_sender,
                rebroadcast_use_tpu,
                &sig_str,
                tx_clone,
                deadline,
                interval_ms,
                max_rebroadcasts,
                start,
                rb_count,
            )
            .await;
        }))
    } else {
        None
    };

    let outcome = {
        #[cfg(windows)]
        {
            let _ = pre_registered_rx;
            let outcome =
                confirm_via_rpc_polling(ctx, signature_base58, deadline, start, slot_at_send).await;
            ctx.pending_tx_confirms.write().remove(signature_base58);
            outcome
        }
        #[cfg(not(windows))]
        {
            confirm_via_jetstream(
                ctx,
                signature_base58,
                deadline,
                start,
                &config,
                pre_registered_rx,
                slot_at_send,
            )
            .await
        }
    };

    // Cancel rebroadcast task on completion
    if let Some(handle) = rebroadcast_handle {
        handle.abort();
    }

    let count = rebroadcast_count.load(Ordering::Relaxed);
    outcome.map(|o| (o, count))
}

/// JetStream-based confirmation: register pending signature, wait for main-loop dispatch or timeout.
async fn confirm_via_jetstream(
    ctx: &ExecutionContext,
    signature_base58: &str,
    deadline: Duration,
    start: std::time::Instant,
    config: &ExecutionConfig,
    pre_registered_rx: Option<tokio::sync::oneshot::Receiver<WalletTxConfirmNotify>>,
    slot_at_send: Option<u64>,
) -> std::result::Result<ConfirmOutcome, String> {
    if !config.jetstream_tx_confirm_enabled {
        ctx.pending_tx_confirms.write().remove(signature_base58);
        TX_CONFIRM_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
        return Ok(ConfirmOutcome::TimeoutSent {
            details: format!(
                "method=jetstream_disabled elapsed_ms={} signature={signature_base58}",
                start.elapsed().as_millis()
            ),
        });
    }

    let notify_rx = match pre_registered_rx {
        Some(rx) => rx,
        None => register_wallet_tx_confirm_waiter(ctx, signature_base58),
    };

    let remaining = deadline.saturating_sub(start.elapsed());

    let outcome = tokio::select! {
        result = notify_rx => {
            match result {
                Ok(confirm) if confirm.err.is_none() => {
                    let elapsed_ms = start.elapsed().as_millis() as u64;
                    TX_CONFIRMED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    TX_CONFIRM_JETSTREAM_TOTAL.fetch_add(1, Ordering::Relaxed);
                    record_confirm_latency_metrics(start, slot_at_send, Some(confirm.slot));
                    Ok(ConfirmOutcome::Confirmed {
                        details: format!(
                            "method=jetstream slot={} elapsed_ms={elapsed_ms} signature={signature_base58}",
                            confirm.slot,
                        ),
                        slot: Some(confirm.slot),
                    })
                }
                Ok(confirm) => {
                    let elapsed_ms = start.elapsed().as_millis() as u64;
                    record_confirm_latency_metrics(start, slot_at_send, Some(confirm.slot));
                    Ok(ConfirmOutcome::FailedConfirmed {
                        details: format!(
                            "method=jetstream err={} slot={} elapsed_ms={elapsed_ms} signature={signature_base58}",
                            confirm.err.as_deref().unwrap_or("unknown"),
                            confirm.slot,
                        ),
                        slot: Some(confirm.slot),
                    })
                }
                Err(_) => {
                    TX_CONFIRM_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
                    Ok(ConfirmOutcome::TimeoutSent {
                        details: format!(
                            "method=jetstream_channel_dropped elapsed_ms={} signature={signature_base58}",
                            start.elapsed().as_millis()
                        ),
                    })
                }
            }
        }
        _ = tokio::time::sleep(remaining) => {
            TX_CONFIRM_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
            Ok(ConfirmOutcome::TimeoutSent {
                details: format!(
                    "method=jetstream_deadline elapsed_ms={} signature={signature_base58}",
                    start.elapsed().as_millis()
                ),
            })
        }
    };

    // Remove pending entry after deadline (late JetStream confirms are ignored — no double-count).
    ctx.pending_tx_confirms.write().remove(signature_base58);

    outcome
}

/// RPC-polling fallback for environments without Geyser/JetStream confirm (e.g. Windows dev/CI).
#[cfg(windows)]
async fn confirm_via_rpc_polling(
    ctx: &ExecutionContext,
    signature_base58: &str,
    deadline: Duration,
    start: std::time::Instant,
    slot_at_send: Option<u64>,
) -> std::result::Result<ConfirmOutcome, String> {
    let signature =
        Signature::from_str(signature_base58).map_err(|e| format!("invalid_signature:{e}"))?;

    let mut attempt: u32 = 0;

    loop {
        if start.elapsed() >= deadline {
            TX_CONFIRM_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Ok(ConfirmOutcome::TimeoutSent {
                details: format!(
                    "method=rpc_polling timeout_ms={} elapsed_ms={} signature={signature_base58}",
                    deadline.as_millis(),
                    start.elapsed().as_millis()
                ),
            });
        }

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
                        "method=rpc_polling err={err:?} confirmations={:?} confirmation_status={:?} elapsed_ms={}",
                        st.confirmations,
                        st.confirmation_status,
                        start.elapsed().as_millis()
                    ),
                    slot: Some(st.slot),
                });
            }

            let config = ctx.config.read();
            let require_finalized = config.confirm_commitment.eq_ignore_ascii_case("finalized");
            drop(config);

            let is_confirmed = match st.confirmation_status {
                Some(solana_transaction_status::TransactionConfirmationStatus::Finalized) => true,
                Some(solana_transaction_status::TransactionConfirmationStatus::Confirmed) => {
                    !require_finalized
                }
                Some(solana_transaction_status::TransactionConfirmationStatus::Processed) => false,
                None => false,
            };

            if is_confirmed {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                TX_CONFIRMED_TOTAL.fetch_add(1, Ordering::Relaxed);
                ironcrab::metrics::TX_CONFIRM_RPC_FALLBACK_TOTAL.fetch_add(1, Ordering::Relaxed);
                record_confirm_latency_metrics(start, slot_at_send, Some(st.slot));
                return Ok(ConfirmOutcome::Confirmed {
                    details: format!(
                        "method=rpc_polling confirmations={:?} confirmation_status={:?} slot={} elapsed_ms={elapsed_ms}",
                        st.confirmations,
                        st.confirmation_status,
                        st.slot,
                    ),
                    slot: Some(st.slot),
                });
            }
        }

        attempt = attempt.saturating_add(1);
        let sleep_ms = (50u64 * attempt.min(20) as u64).min(1_000);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}

/// Periodic rebroadcast loop running in a parallel task.
/// Aborted by the caller when confirmation arrives.
#[allow(clippy::too_many_arguments)]
async fn spawn_rebroadcast_loop(
    rpc: Arc<ironcrab::solana::rpc::SolanaRpc>,
    tx_sender: Option<Arc<TxSender>>,
    rebroadcast_use_tpu: bool,
    signature_base58: &str,
    vtx: VersionedTransaction,
    deadline: Duration,
    interval_ms: u64,
    max_rebroadcasts: u32,
    confirm_start: std::time::Instant,
    rebroadcast_count: Arc<std::sync::atomic::AtomicU32>,
) {
    let loop_start = std::time::Instant::now();
    let mut rebroadcasts: u32 = 0;
    let interval = Duration::from_millis(interval_ms);

    // Wait one interval before first rebroadcast
    tokio::time::sleep(interval).await;

    while rebroadcasts < max_rebroadcasts && loop_start.elapsed() < deadline {
        let mut sent = false;

        if rebroadcast_use_tpu {
            if let Some(ref sender) = tx_sender {
                match sender.send_versioned_tpu_then_rpc(&vtx).await {
                    Ok(result) => {
                        rebroadcasts += 1;
                        rebroadcast_count.store(rebroadcasts, Ordering::Relaxed);
                        record_tx_rebroadcast();
                        record_tx_rebroadcast_during_confirm_ms(
                            confirm_start.elapsed().as_millis() as u64,
                        );
                        let method = result.method.to_string();
                        record_tx_rebroadcast_method(&method);
                        info!(
                            signature = %result.signature,
                            original_signature = %signature_base58,
                            rebroadcasts = rebroadcasts,
                            method = %method,
                            "Rebroadcasted TX during confirmation wait"
                        );
                        sent = true;
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            original_signature = %signature_base58,
                            rebroadcasts = rebroadcasts + 1,
                            "Rebroadcast TxSender failed, falling back to RPC"
                        );
                    }
                }
            }
        }

        if !sent {
            let cfg = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: None,
                encoding: Some(UiTransactionEncoding::Base64),
                max_retries: Some(3),
                min_context_slot: None,
            };

            match rpc.rpc.send_transaction_with_config(&vtx, cfg).await {
                Ok(sig) => {
                    rebroadcasts += 1;
                    rebroadcast_count.store(rebroadcasts, Ordering::Relaxed);
                    record_tx_rebroadcast();
                    record_tx_rebroadcast_during_confirm_ms(
                        confirm_start.elapsed().as_millis() as u64
                    );
                    record_tx_rebroadcast_method("rpc");
                    info!(
                        signature = %sig,
                        original_signature = %signature_base58,
                        rebroadcasts = rebroadcasts,
                        method = "rpc",
                        "Rebroadcasted TX during confirmation wait"
                    );
                }
                Err(e) => {
                    rebroadcasts += 1;
                    rebroadcast_count.store(rebroadcasts, Ordering::Relaxed);
                    record_tx_rebroadcast();
                    record_tx_rebroadcast_during_confirm_ms(
                        confirm_start.elapsed().as_millis() as u64
                    );
                    record_tx_rebroadcast_method("rpc");
                    warn!(
                        error = %e,
                        original_signature = %signature_base58,
                        rebroadcasts = rebroadcasts,
                        method = "rpc",
                        "Rebroadcast attempt failed"
                    );
                }
            }
        }

        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod execution_engine_tests {
    use super::{
        apply_scope48_confirmed_sell_execution_metadata, build_signed_versioned_tx,
        build_unsigned_versioned_tx, cold_path_dex_sim_failure_triggers_discovery_recovery,
        compute_pre_send_capital_lock_ttl, effective_intent_ttl_ms, intent_is_expired,
        is_cold_path_recovery_sell, is_pump_amm_structural_sim_error,
        is_pumpfun_bonding_curve_structural_sim_error, is_regular_momentum_hot_path_sell,
        liquidation_lockmanager_seed_decision, liquidation_pumpfun_sell_preference,
        liquidation_store_multi_pool_fallback_metadata, max_open_positions_buy_gate,
        prepare_unsigned_versioned_tx_for_simulation,
        pump_amm_hint_pool_cache_usable_for_tx_plan_builder,
        pump_amm_hot_path_quote_not_ready_detail, pump_amm_liquidation_discovery_force_refresh,
        pump_amm_liquidation_quote_timeout_str, pump_amm_pool_market_hint_merge,
        pump_amm_slave_recovery_snapshot, record_pump_amm_hot_path_refresh_after_success,
        scale_in_max_open_positions_skip_details, scope48_confirmed_sell_close_decision,
        sell_token_balance_gate, should_publish_open_position_pool_pin_after_confirmed_buy,
        sim_failure_reject_reason, simulation_result_on_rpc_timeout,
        sort_route_candidates_by_amount_out, take_next_multi_pool_buildable_fallback_route,
        try_pump_amm_hot_path_refresh_publish, wait_for_meteora_dlmm_slave_after_recovery,
        wait_for_pump_amm_pool_hint_ready_for_tx_plan_builder,
        wait_for_pump_amm_slave_after_recovery, wait_for_pumpfun_bonding_cache_refresh,
        ComputedIntentFills, DiscoveryRequestOutcome, ExecutionConfig, LiquidationSeedDecision,
        PumpAmmHotPathRefreshDecision, RouteCandidate, PUMP_AMM_HOT_PATH_REFRESH_COOLDOWN,
        WSOL_MINT,
    };
    use ironcrab::execution::live_pool_cache::{
        CachedPoolState, LivePoolCache, MeteoraState, PumpAmmState, PumpFunState,
    };
    use ironcrab::execution::pool_cache_sync::apply_pool_cache_update;
    use ironcrab::execution::tx_builder::TxPlan;
    use ironcrab::execution::wsol_manager::PendingWrapState;
    use ironcrab::ipc::MarketEvent;
    use ironcrab::ipc::RejectReason;
    use ironcrab::ipc::{
        CheckResult, ControlResponse, ControlResponseStatus, DecisionOutcome, DecisionRecord,
        DexPoolReadiness, ExecutionResult, ExecutionStatus, ExplicitAmount, FillStatus,
        IntentOrigin, IntentTier, MarketEventKind, PoolCacheUpdate, TradeIntent, TradeResources,
        TradeSide, TradingRegime,
    };
    use ironcrab::position_authority::{
        wallet_bootstrap_allows_pa_kv_tombstone_sweep, PositionAuthority, PositionEvent,
    };
    use ironcrab::solana::address_lookup_table::LoadedAlt;
    use ironcrab::solana::dex::pumpfun::PumpFunDex;
    use ironcrab::storage::locks::{LockHolder, LockManager, LockResult};
    use parking_lot::Mutex as ParkingMutex;
    use solana_message::VersionedMessage;
    use solana_sdk::{
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        pubkey::Pubkey,
        signature::{Keypair, Signer},
    };
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Instant;

    fn test_system_transfer(from: &Pubkey, to: &Pubkey, lamports: u64) -> Instruction {
        let mut data = vec![2, 0, 0, 0];
        data.extend_from_slice(&lamports.to_le_bytes());
        Instruction {
            program_id: solana_system_program::id(),
            accounts: vec![AccountMeta::new(*from, true), AccountMeta::new(*to, false)],
            data,
        }
    }

    #[test]
    fn sim_and_send_share_identical_message_without_alt() {
        let wallet = Keypair::new();
        let blockhash = Hash::new_unique();
        let recipient = Pubkey::new_unique();
        let plan = TxPlan {
            instructions: vec![test_system_transfer(&wallet.pubkey(), &recipient, 1)],
        };

        let unsigned = build_unsigned_versioned_tx(&wallet.pubkey(), &plan, blockhash, None)
            .expect("unsigned legacy tx");
        let signed =
            build_signed_versioned_tx(&wallet.pubkey(), &plan, blockhash, None, &[&wallet])
                .expect("signed legacy tx");

        assert_eq!(unsigned.message, signed.message);
        assert!(matches!(unsigned.message, VersionedMessage::Legacy(_)));
    }

    fn wsol_wallet_snapshot_event(balance_raw: u64) -> MarketEvent {
        MarketEvent::new(
            "test",
            "test",
            "run",
            "evt-wsol-snapshot".to_string(),
            "geyser",
            None,
            MarketEventKind::WalletBalanceSnapshot {
                mint: WSOL_MINT.to_string(),
                balance_raw,
                decimals: 9,
                token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            },
        )
    }

    fn test_ctx_with_pending_wrap(
        lock_manager: LockManager,
    ) -> (super::ExecutionContext, Arc<PendingWrapState>) {
        let pending = Arc::new(PendingWrapState::new());
        let mut ctx =
            super::ExecutionContext::test_for_pa2_metrics(lock_manager, PositionAuthority::new());
        ctx.wsol_pending_wrap = Some(Arc::clone(&pending));
        (ctx, pending)
    }

    #[test]
    fn lock_manager_pending_wrap_floors_stale_zero_snapshot() {
        let (ctx, pending) = test_ctx_with_pending_wrap(LockManager::new(3_000_000_000));
        pending.arm(1_000_000_000);
        ctx.lock_manager.update_wsol_only(1_000_000_000);

        ctx.apply_wallet_balance_snapshot_event(&wsol_wallet_snapshot_event(0));

        assert_eq!(ctx.lock_manager.available_wsol(), 1_000_000_000);
        assert_eq!(pending.pending_expected(), 1_000_000_000);
    }

    #[test]
    fn lock_manager_pending_wrap_confirms_on_matching_snapshot() {
        let (ctx, pending) = test_ctx_with_pending_wrap(LockManager::new(3_000_000_000));
        pending.arm(1_000_000_000);
        ctx.lock_manager.update_wsol_only(0);

        ctx.apply_wallet_balance_snapshot_event(&wsol_wallet_snapshot_event(1_000_000_000));

        assert_eq!(ctx.lock_manager.available_wsol(), 1_000_000_000);
        assert_eq!(pending.pending_expected(), 0);
    }

    #[test]
    fn lock_manager_without_pending_accepts_zero_unwrap_snapshot() {
        let ctx = super::ExecutionContext::test_for_pa2_metrics(
            LockManager::new(3_000_000_000),
            PositionAuthority::new(),
        );
        ctx.lock_manager.update_wsol_only(1_000_000_000);

        ctx.apply_wallet_balance_snapshot_event(&wsol_wallet_snapshot_event(0));

        assert_eq!(ctx.lock_manager.available_wsol(), 0);
    }

    #[test]
    fn execution_config_default_disables_position_authority_kv_publish() {
        let cfg = ExecutionConfig::default();
        assert!(
            !cfg.publish_position_authority_kv,
            "PA-6b: EE must not publish PositionAuthority KV by default"
        );
    }

    #[test]
    fn wallet_bootstrap_tombstone_sweep_requires_snapshot_complete() {
        let balance_only = vec![MarketEventKind::WalletBalanceSnapshot {
            mint: "mintA".to_string(),
            balance_raw: 1,
            decimals: 6,
            token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
        }];
        assert!(
            !wallet_bootstrap_allows_pa_kv_tombstone_sweep(&balance_only),
            "partial balance snapshots must not authorize tombstone sweep"
        );
        assert!(
            !wallet_bootstrap_allows_pa_kv_tombstone_sweep(&[]),
            "empty bootstrap must not authorize tombstone sweep"
        );
        let with_complete = vec![MarketEventKind::WalletSnapshotComplete {
            wallet: "wallet".to_string(),
            mints_in_wallet: vec!["mintA".to_string()],
            is_periodic: true,
        }];
        assert!(
            wallet_bootstrap_allows_pa_kv_tombstone_sweep(&with_complete),
            "WalletSnapshotComplete authorizes tombstone sweep"
        );
    }

    #[test]
    fn sim_and_send_share_identical_message_with_alt() {
        let wallet = Keypair::new();
        let recipient = Pubkey::new_unique();
        let blockhash = Hash::new_unique();
        let alt = LoadedAlt {
            address: Pubkey::new_unique(),
            accounts: vec![recipient],
        };
        let plan = TxPlan {
            instructions: vec![test_system_transfer(&wallet.pubkey(), &recipient, 1)],
        };

        let unsigned = build_unsigned_versioned_tx(&wallet.pubkey(), &plan, blockhash, Some(&alt))
            .expect("unsigned v0 tx");
        let signed =
            build_signed_versioned_tx(&wallet.pubkey(), &plan, blockhash, Some(&alt), &[&wallet])
                .expect("signed v0 tx");

        assert_eq!(unsigned.message, signed.message);
        assert!(matches!(unsigned.message, VersionedMessage::V0(_)));
    }

    fn oversized_legacy_tx_plan(wallet: &Keypair) -> (TxPlan, Hash) {
        let blockhash = Hash::new_unique();
        let mut instructions = Vec::new();
        for _ in 0..40 {
            instructions.push(test_system_transfer(
                &wallet.pubkey(),
                &Pubkey::new_unique(),
                1,
            ));
        }
        (TxPlan { instructions }, blockhash)
    }

    #[test]
    fn tx_size_check_rejects_oversized_serialized_transaction_on_send_path() {
        let wallet = Keypair::new();
        let (plan, blockhash) = oversized_legacy_tx_plan(&wallet);
        let tx = build_signed_versioned_tx(&wallet.pubkey(), &plan, blockhash, None, &[&wallet])
            .unwrap();
        let err = super::check_versioned_tx_size(&tx)
            .expect_err("oversized legacy tx should be rejected on send");
        assert!(err.starts_with("tx_too_large:"));
    }

    #[test]
    fn oversized_tx_rejected_before_simulation() {
        let wallet = Keypair::new();
        let (plan, blockhash) = oversized_legacy_tx_plan(&wallet);
        let err =
            prepare_unsigned_versioned_tx_for_simulation(&wallet.pubkey(), &plan, blockhash, None)
                .expect_err("oversized legacy tx should be rejected before simulation RPC");
        assert!(err.starts_with("tx_too_large:"));
    }

    #[test]
    fn simulation_rpc_timeout_maps_to_sim_failed_with_sim_timeout_code() {
        let result = simulation_result_on_rpc_timeout();
        assert!(!result.success);
        assert_eq!(result.error_code.as_deref(), Some("sim_timeout"));
        assert_eq!(
            sim_failure_reject_reason(result.error_code.as_deref()),
            RejectReason::SimTimeout
        );
    }

    #[test]
    fn sim_failure_reject_reason_defaults_to_sim_failed_for_other_errors() {
        assert_eq!(
            sim_failure_reject_reason(Some("rpc_error:timeout")),
            RejectReason::SimFailed
        );
        assert_eq!(sim_failure_reject_reason(None), RejectReason::SimFailed);
    }

    #[test]
    fn intent_ttl_expired_when_now_past_header_ts_plus_ttl() {
        let intent_ts = 1_000_000u64;
        let ttl = 5_000u64;
        assert!(intent_is_expired(
            intent_ts + ttl + 1,
            intent_ts,
            Some(ttl),
            5_000
        ));
    }

    #[test]
    fn intent_ttl_passes_when_within_window() {
        let intent_ts = 1_000_000u64;
        let ttl = 5_000u64;
        assert!(!intent_is_expired(
            intent_ts + ttl,
            intent_ts,
            Some(ttl),
            5_000
        ));
        assert!(!intent_is_expired(intent_ts, intent_ts, Some(ttl), 5_000));
    }

    #[test]
    fn pre_send_capital_lock_ttl_covers_cold_path_discovery_plus_sim_plus_buffer() {
        let config = ExecutionConfig::default();
        let ttl = compute_pre_send_capital_lock_ttl(&config);
        // 45s liquidation + 20s SLAVE + 500ms sim + 10s buffer = 75.5s
        assert!(
            ttl.as_millis() >= 75_000,
            "TTL must cover 65s+ cold path; got {}ms",
            ttl.as_millis()
        );
        assert!(
            ttl.as_millis() > 30_000,
            "must exceed legacy 30s default lock TTL"
        );
    }

    /// Mirrors `process_intent` fill cache: second consumer must not re-invoke the compute closure.
    fn cached_or_compute_intent_fills_for_test(
        cache: &mut Option<ComputedIntentFills>,
        compute: impl FnOnce() -> ComputedIntentFills,
    ) -> &ComputedIntentFills {
        cache.get_or_insert_with(compute)
    }

    #[test]
    fn fill_computation_runs_once_per_signature_cache() {
        let mut cache: Option<ComputedIntentFills> = None;
        let mut calls = 0u32;
        cached_or_compute_intent_fills_for_test(&mut cache, || {
            calls += 1;
            ComputedIntentFills {
                fill_in: None,
                fill_out: None,
                fill_status: FillStatus::Unavailable,
                fill_unavailable_reason: None,
                wallet_sol_delta: None,
                block_time_unix_ms: Some(42),
                wallet_token_balance_absent_after_tx: false,
            }
        });
        cached_or_compute_intent_fills_for_test(&mut cache, || {
            calls += 1;
            ComputedIntentFills {
                fill_in: None,
                fill_out: None,
                fill_status: FillStatus::Unavailable,
                fill_unavailable_reason: None,
                wallet_sol_delta: None,
                block_time_unix_ms: Some(99),
                wallet_token_balance_absent_after_tx: false,
            }
        });
        assert_eq!(calls, 1);
        assert_eq!(cache.as_ref().unwrap().block_time_unix_ms, Some(42));
    }

    #[test]
    fn intent_ttl_uses_engine_default_when_intent_ttl_missing_or_zero() {
        assert_eq!(effective_intent_ttl_ms(None, 5_000), 5_000);
        assert_eq!(effective_intent_ttl_ms(Some(0), 5_000), 5_000);
        let intent_ts = 10_000u64;
        assert!(intent_is_expired(intent_ts + 5_001, intent_ts, None, 5_000));
        assert!(!intent_is_expired(
            intent_ts + 5_000,
            intent_ts,
            Some(0),
            5_000
        ));
    }

    #[test]
    fn expired_intent_decision_record_uses_expired_outcome() {
        let checks = vec![CheckResult {
            check_name: "ttl_valid".to_string(),
            passed: false,
            reason_code: Some(RejectReason::TtlExpired.to_string()),
            details: Some("now_ms=20000 intent_ts_ms=10000 ttl_ms=5000".to_string()),
        }];
        let mut decision = DecisionRecord::new_rejected(
            "execution-engine",
            "test",
            "run",
            "dec-1".to_string(),
            "int-1".to_string(),
            "momentum-bot".to_string(),
            IntentOrigin::StrategyA,
            TradingRegime::Early,
            checks,
            RejectReason::TtlExpired.to_string(),
        );
        decision.outcome = DecisionOutcome::Expired;
        assert_eq!(decision.outcome, DecisionOutcome::Expired);
        assert_eq!(
            decision.primary_reject_reason.as_deref(),
            Some(RejectReason::TtlExpired.as_str())
        );
        let ttl_check = decision
            .checks
            .iter()
            .find(|c| c.check_name == "ttl_valid")
            .expect("ttl check present");
        assert!(!ttl_check.passed);
    }

    #[test]
    fn max_open_positions_gate_rejects_when_authority_exceeds_max() {
        let max_open = 5usize;
        let (passed, details) = max_open_positions_buy_gate(1, Some(5), 1, max_open);
        assert!(!passed);
        assert!(details.contains("authority_current=5"));
        assert!(details.contains("lockmanager_current=1"));
        assert!(details.contains("metadata_current=1"));
        assert!(details.contains("effective_current=5"));
        assert!(details.contains("source=position_authority(+lockmanager)"));
        assert!(details.contains("max=5"));
    }

    #[test]
    fn max_open_positions_gate_rejects_when_lock_manager_exceeds_metadata() {
        let max_open = 5usize;
        let (passed, details) = max_open_positions_buy_gate(1, Some(1), 5, max_open);
        assert!(!passed);
        assert!(details.contains("authority_current=1"));
        assert!(details.contains("lockmanager_current=5"));
        assert!(details.contains("metadata_current=1"));
        assert!(details.contains("effective_current=5"));
        assert!(details.contains("max=5"));
    }

    #[test]
    fn max_open_positions_gate_rejects_when_metadata_exceeds_authority_and_lockmanager() {
        let max_open = 5usize;
        let (passed, details) = max_open_positions_buy_gate(5, Some(1), 1, max_open);
        assert!(!passed);
        assert!(details.contains("metadata_current=5"));
        assert!(details.contains("authority_current=1"));
        assert!(details.contains("lockmanager_current=1"));
        assert!(details.contains("effective_current=5"));
    }

    #[test]
    fn max_open_positions_gate_passes_when_all_counts_below_max() {
        let max_open = 5usize;
        let (passed, details) = max_open_positions_buy_gate(2, Some(3), 1, max_open);
        assert!(passed);
        assert!(details.contains("authority_current=3"));
        assert!(details.contains("lockmanager_current=1"));
        assert!(details.contains("metadata_current=2"));
        assert!(details.contains("effective_current=3"));
        assert!(details.contains("< max=5"));
        assert!(details.contains("source=position_authority(+lockmanager)"));
    }

    #[test]
    fn max_open_positions_gate_fallback_when_authority_unavailable() {
        let max_open = 5usize;
        let (passed, details) = max_open_positions_buy_gate(1, None, 5, max_open);
        assert!(!passed);
        assert!(details.contains("authority_unavailable=true"));
        assert!(details.contains("lockmanager_current=5"));
        assert!(details.contains("metadata_current=1"));
        assert!(details.contains("effective_current=5"));
        assert!(details.contains("source=lockmanager"));
    }

    #[test]
    fn max_open_positions_gate_skipped_for_scale_in_metadata() {
        const MINT: &str = "ScaleInMintP179";
        let lm = LockManager::new(0);
        lm.set_available_token_balance(MINT.to_string(), 1_000_000);
        let skip = scale_in_max_open_positions_skip_details(Some("scale_in"), MINT, &lm);
        assert!(skip.is_some());
        let details = skip.unwrap();
        assert!(details.contains("skipped_for_scale_in"));
        assert!(details.contains("existing_balance_raw=1000000"));
        let (passed, _) = max_open_positions_buy_gate(10, Some(10), 10, 5);
        assert!(
            !passed,
            "gate would reject at limit; skip must bypass in process_intent"
        );
    }

    #[test]
    fn max_open_positions_gate_still_rejects_probe_at_limit() {
        let max_open = 5usize;
        let (passed, details) = max_open_positions_buy_gate(10, Some(10), 10, max_open);
        assert!(!passed);
        assert!(details.contains("effective_current=10"));
        let lm = LockManager::new(0);
        assert!(scale_in_max_open_positions_skip_details(Some("probe"), "AnyMint", &lm).is_none());
    }

    #[test]
    fn max_open_positions_gate_scale_in_without_balance_applies_gate() {
        let lm = LockManager::new(0);
        assert!(
            scale_in_max_open_positions_skip_details(Some("scale_in"), "NoBalanceMint", &lm)
                .is_none()
        );
        let (passed, details) = max_open_positions_buy_gate(10, Some(10), 10, 5);
        assert!(!passed);
        assert!(details.contains(">="));
    }

    const SELL_GATE_MINT: &str = "SellGateMint111111111111111111111111111";

    fn pa_buy(mint: &str, raw: u64) -> PositionEvent {
        PositionEvent::BuyConfirmed {
            mint: mint.to_string(),
            fill_raw: raw,
            decimals: 6,
            token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            ata: None,
        }
    }

    fn pa_sell(mint: &str, raw: u64) -> PositionEvent {
        PositionEvent::SellConfirmed {
            mint: mint.to_string(),
            sold_raw: raw,
            decimals: 6,
            token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
        }
    }

    #[test]
    fn primary_open_positions_gauge_uses_authority_not_lockmanager_ghost() {
        use super::ExecutionContext;
        use crate::{
            OPEN_POSITIONS_GAUGE, POSITION_AUTHORITY_LOCKMANAGER_OPEN_GAUGE,
            POSITION_AUTHORITY_OPEN_GAUGE,
        };
        use std::sync::atomic::Ordering;

        const M1: &str = "MintA1111111111111111111111111111111111111";
        const M2: &str = "MintB1111111111111111111111111111111111111";
        const M3: &str = "MintC1111111111111111111111111111111111111";

        let lm = LockManager::new(1_000_000_000);
        lm.set_available_token_balance(M1.to_string(), 100);
        lm.set_available_token_balance(M2.to_string(), 200);
        lm.set_available_token_balance(M3.to_string(), 300);

        let mut pa = PositionAuthority::new();
        pa.apply(&pa_buy(M1, 100));

        let ctx = ExecutionContext::test_for_pa2_metrics(lm, pa);
        ctx.refresh_position_authority_metrics();

        assert_eq!(POSITION_AUTHORITY_OPEN_GAUGE.load(Ordering::Relaxed), 1);
        assert_eq!(OPEN_POSITIONS_GAUGE.load(Ordering::Relaxed), 1);
        assert_eq!(
            POSITION_AUTHORITY_LOCKMANAGER_OPEN_GAUGE.load(Ordering::Relaxed),
            3
        );

        {
            let mut pa = ctx.position_authority.lock();
            pa.apply(&pa_sell(M1, 100));
        }
        ctx.refresh_position_authority_metrics();

        assert_eq!(OPEN_POSITIONS_GAUGE.load(Ordering::Relaxed), 0);
        assert_eq!(POSITION_AUTHORITY_OPEN_GAUGE.load(Ordering::Relaxed), 0);
        assert_eq!(
            POSITION_AUTHORITY_LOCKMANAGER_OPEN_GAUGE.load(Ordering::Relaxed),
            3,
            "lockmanager ghost gauge must still reflect non-zero balances"
        );
    }

    #[test]
    fn sell_token_balance_gate_rejects_when_authority_tradable_below_required() {
        let (passed, details, effective) =
            sell_token_balance_gate(1_000_000, Some(500_000), 600_000, SELL_GATE_MINT);
        assert!(!passed);
        assert_eq!(effective, 500_000);
        assert!(details.contains("authority_tradable=500000"));
        assert!(details.contains("lockmanager_available=1000000"));
        assert!(details.contains("effective=500000"));
        assert!(details.contains("required=600000"));
        assert!(details.contains("source=position_authority(+lockmanager)"));
    }

    #[test]
    fn sell_token_balance_gate_rejects_ghost_lock_when_authority_closed() {
        let (passed, details, effective) =
            sell_token_balance_gate(1_000_000, Some(0), 100, SELL_GATE_MINT);
        assert!(!passed);
        assert_eq!(effective, 0);
        assert!(details.contains("authority_tradable=0"));
        assert!(details.contains("lockmanager_available=1000000"));
        assert!(details.contains("source=position_authority(+lockmanager)"));
    }

    #[test]
    fn sell_token_balance_gate_passes_lockmanager_fallback_when_authority_unknown() {
        let (passed, details, effective) =
            sell_token_balance_gate(1_000_000, None, 500_000, SELL_GATE_MINT);
        assert!(passed);
        assert_eq!(effective, 1_000_000);
        assert!(details.contains("authority_tradable=unknown"));
        assert!(details.contains("source=lockmanager_fallback"));
    }

    #[test]
    fn sell_token_balance_gate_passes_when_authority_and_lock_match() {
        let (passed, details, effective) =
            sell_token_balance_gate(1_000_000, Some(1_000_000), 1_000_000, SELL_GATE_MINT);
        assert!(passed);
        assert_eq!(effective, 1_000_000);
        assert!(details.contains("effective=1000000"));
        assert!(details.contains("required=1000000"));
    }

    #[test]
    fn liquidation_seed_skipped_when_authority_closed_or_zero() {
        let mut pa = PositionAuthority::new();
        pa.apply(&pa_buy(SELL_GATE_MINT, 1_000_000));
        pa.apply(&pa_sell(SELL_GATE_MINT, 1_000_000));
        assert_eq!(
            liquidation_lockmanager_seed_decision(&pa, SELL_GATE_MINT, 500_000),
            LiquidationSeedDecision::Skip
        );
    }

    #[test]
    fn liquidation_seed_capped_to_authority_tradable() {
        let mut pa = PositionAuthority::new();
        pa.apply(&pa_buy(SELL_GATE_MINT, 400_000));
        assert_eq!(
            liquidation_lockmanager_seed_decision(&pa, SELL_GATE_MINT, 1_000_000),
            LiquidationSeedDecision::Seed(400_000)
        );
    }

    #[test]
    fn liquidation_seed_uses_rpc_when_authority_unknown() {
        let pa = PositionAuthority::new();
        assert_eq!(
            liquidation_lockmanager_seed_decision(&pa, SELL_GATE_MINT, 1_000_000),
            LiquidationSeedDecision::Seed(1_000_000)
        );
    }

    /// Scope 51: timeout log text matches `PUMPSWAP_LIQUIDATION_QUOTE_TIMEOUT_SECS` (45s).
    #[test]
    fn pump_amm_liquidation_timeout_log_uses_45s_not_10s() {
        let s = pump_amm_liquidation_quote_timeout_str();
        assert!(s.contains("45s"), "expected 45s in timeout string, got {s}");
        assert!(!s.contains("10s"), "stale 10s label must not appear: {s}");
    }

    /// P184m / I-24e: liquidation discovery must force_refresh (no cache-first stale SLAVE reuse).
    #[test]
    fn pump_amm_liquidation_discovery_uses_force_refresh() {
        assert!(pump_amm_liquidation_discovery_force_refresh());
    }

    /// P184m: hot-path PumpSwap SELL skips simulation when quote is not ready.
    #[test]
    fn pump_amm_hot_path_quote_not_ready_gates_simulation() {
        let base_mint = Pubkey::new_unique();
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        cache.upsert(
            pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint: Pubkey::new_unique(),
                pool_base_token_account: Pubkey::default(),
                pool_quote_token_account: Pubkey::default(),
                base_reserve: Some(0),
                quote_reserve: Some(0),
                pool_accounts: (0..14).map(|_| Pubkey::new_unique()).collect(),
                creator: None,
            }),
            0,
        );

        let mut intent = TradeIntent::new_sell(
            "momentum-bot",
            "0.1.0",
            "run",
            "int-1".to_string(),
            "momentum-bot",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            base_mint.to_string(),
            6,
            WSOL_MINT.to_string(),
            1_000,
            0,
            500,
            TradingRegime::Established,
        );
        intent
            .metadata
            .insert("dex".to_string(), "pump_amm".to_string());

        let detail = pump_amm_hot_path_quote_not_ready_detail(&intent, Some(&cache));
        assert!(
            detail
                .as_deref()
                .is_some_and(|d| d.contains("quote_not_ready")),
            "expected quote_not_ready gate, got {detail:?}"
        );

        let mut ready = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            intent.resources.output_mint.clone(),
            1_000,
            2_000,
            None,
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "pool_accounts".to_string(),
            (0..14)
                .map(|_| Pubkey::new_unique().to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        ready.metadata = Some(meta);
        ready.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        apply_pool_cache_update(&cache, &ready);

        assert!(
            pump_amm_hot_path_quote_not_ready_detail(&intent, Some(&cache)).is_none(),
            "ready quote must not gate simulation"
        );
    }

    /// Known active curve (`complete=false`): prefer PumpFun before multi-pool.
    #[test]
    fn liquidation_pumpfun_sell_preference_true_when_incomplete_bonding_curve() {
        let base_mint = Pubkey::new_unique();
        let bc = Pubkey::new_unique();
        let cache = LivePoolCache::new();
        cache.upsert(
            bc,
            CachedPoolState::PumpFun(PumpFunState {
                token_mint: base_mint,
                bonding_curve: bc,
                associated_bonding_curve: Pubkey::new_unique(),
                virtual_sol_reserves: 0,
                virtual_token_reserves: 0,
                real_sol_reserves: 0,
                real_token_reserves: 0,
                complete: false,
                creator: Pubkey::new_unique(),
                cashback_enabled: false,
            }),
            0,
        );
        assert!(liquidation_pumpfun_sell_preference(
            Some(&cache),
            &base_mint
        ));
    }

    /// Migrated/complete: do not prefer PumpFun first (use multi-pool + fallback).
    #[test]
    fn liquidation_pumpfun_sell_preference_false_when_migrated() {
        let base_mint = Pubkey::new_unique();
        let bc = Pubkey::new_unique();
        let cache = LivePoolCache::new();
        cache.upsert(
            bc,
            CachedPoolState::PumpFun(PumpFunState {
                token_mint: base_mint,
                bonding_curve: bc,
                associated_bonding_curve: Pubkey::new_unique(),
                virtual_sol_reserves: 0,
                virtual_token_reserves: 0,
                real_sol_reserves: 0,
                real_token_reserves: 0,
                complete: true,
                creator: Pubkey::new_unique(),
                cashback_enabled: false,
            }),
            0,
        );
        assert!(!liquidation_pumpfun_sell_preference(
            Some(&cache),
            &base_mint
        ));
    }

    /// No PumpFun row in cache: safe default = multi-pool first (unknown migration state).
    #[test]
    fn liquidation_pumpfun_sell_preference_false_without_cache_state() {
        let base_mint = Pubkey::new_unique();
        let cache = LivePoolCache::new();
        assert!(!liquidation_pumpfun_sell_preference(None, &base_mint));
        assert!(!liquidation_pumpfun_sell_preference(
            Some(&cache),
            &base_mint
        ));
    }

    #[test]
    fn cold_path_liquidation_sim_any_error_triggers_pump_amm_recovery_gate() {
        let mut intent = TradeIntent::new(
            "liq",
            "v0.1.0",
            "run-1",
            "id-liq".to_string(),
            "momentum-bot",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(1_000_000, 6),
            TradeResources {
                input_mint: "Mint111111111111111111111111111111111111111".to_string(),
                output_mint: ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
                pools: vec!["Pool123".to_string()],
                accounts: vec![],
                token_program: None,
            },
            0,
            200,
            TradeSide::Sell,
            TradingRegime::Early,
        );
        intent
            .metadata
            .insert("purpose".to_string(), "liquidation".to_string());
        assert!(cold_path_dex_sim_failure_triggers_discovery_recovery(
            &intent,
            Some("Custom(6004)"),
            is_pump_amm_structural_sim_error
        ));
        assert!(cold_path_dex_sim_failure_triggers_discovery_recovery(
            &intent,
            None,
            is_pump_amm_structural_sim_error
        ));
    }

    #[test]
    fn cold_path_sell_all_only_non_structural_sim_does_not_trigger_pump_amm_recovery_gate() {
        let mut intent = TradeIntent::new(
            "sell-all",
            "v0.1.0",
            "run-1",
            "id-sa".to_string(),
            "sell-all",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(1_000_000, 6),
            TradeResources {
                input_mint: "Mint111111111111111111111111111111111111111".to_string(),
                output_mint: ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
                pools: vec!["Pool123".to_string()],
                accounts: vec![],
                token_program: None,
            },
            0,
            200,
            TradeSide::Sell,
            TradingRegime::Early,
        );
        intent
            .metadata
            .insert("sell_all".to_string(), "true".to_string());
        assert!(!cold_path_dex_sim_failure_triggers_discovery_recovery(
            &intent,
            Some("Custom(6004)"),
            is_pump_amm_structural_sim_error
        ));
        assert!(cold_path_dex_sim_failure_triggers_discovery_recovery(
            &intent,
            Some("Custom(6023)"),
            is_pump_amm_structural_sim_error
        ));
    }

    #[test]
    fn pump_amm_structural_sim_error_matches_cold_path_recovery_pattern() {
        assert!(is_pump_amm_structural_sim_error(Some(
            "Simulation failed: Custom(6023)"
        )));
        assert!(is_pump_amm_structural_sim_error(Some(
            "Simulation failed: Custom(6013)"
        )));
        assert!(is_pump_amm_structural_sim_error(Some(
            "InvalidProtocolFeeRecipient"
        )));
        assert!(is_pump_amm_structural_sim_error(Some("Overflow")));
        assert!(is_pump_amm_structural_sim_error(Some("0x1787")));
        assert!(is_pump_amm_structural_sim_error(Some("0x177d")));
        assert!(is_pump_amm_structural_sim_error(Some("0x177D")));
        assert!(!is_pump_amm_structural_sim_error(Some("Custom(6005)")));
        assert!(!is_pump_amm_structural_sim_error(None));
    }

    #[test]
    fn pumpfun_bonding_structural_sim_error_matches_cold_path_recovery_pattern() {
        assert!(is_pumpfun_bonding_curve_structural_sim_error(Some(
            "Simulation failed: Custom(6023)"
        )));
        assert!(is_pumpfun_bonding_curve_structural_sim_error(Some(
            "Custom(6024)"
        )));
        assert!(is_pumpfun_bonding_curve_structural_sim_error(Some(
            "Overflow"
        )));
        assert!(is_pumpfun_bonding_curve_structural_sim_error(Some(
            "0x1787"
        )));
        assert!(is_pumpfun_bonding_curve_structural_sim_error(Some(
            "0x1788"
        )));
        assert!(!is_pumpfun_bonding_curve_structural_sim_error(Some(
            "Custom(6005)"
        )));
        assert!(!is_pumpfun_bonding_curve_structural_sim_error(None));
    }

    #[test]
    fn regular_momentum_hot_path_sell_requires_momentum_source_and_not_sell_all() {
        let mut intent = TradeIntent::new(
            "momentum-bot",
            "v0.1.0",
            "run-1",
            "id-1".to_string(),
            "momentum-bot",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(1_000_000, 6),
            TradeResources {
                input_mint: "Mint111111111111111111111111111111111111111".to_string(),
                output_mint: ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
                pools: vec!["Pool123".to_string()],
                accounts: vec![],
                token_program: None,
            },
            0,
            200,
            TradeSide::Sell,
            TradingRegime::Early,
        );
        assert!(is_regular_momentum_hot_path_sell(&intent));

        intent.source = "sell-all".to_string();
        assert!(!is_regular_momentum_hot_path_sell(&intent));

        intent.source = "momentum-bot".to_string();
        intent
            .metadata
            .insert("sell_all".to_string(), "true".to_string());
        assert!(!is_regular_momentum_hot_path_sell(&intent));
    }

    #[test]
    fn open_position_pool_pin_filters_arb_sol_and_requires_momentum_buy() {
        let token_mint = "Mint111111111111111111111111111111111111111".to_string();
        let pool = "Pool123".to_string();
        let mut intent = TradeIntent::new(
            "momentum-bot",
            "v0.1.0",
            "run-1",
            "id-buy".to_string(),
            "momentum-bot",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(1_000_000, 6),
            TradeResources {
                input_mint: ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
                output_mint: token_mint.clone(),
                pools: vec![pool.clone()],
                accounts: vec![],
                token_program: None,
            },
            0,
            200,
            TradeSide::Buy,
            TradingRegime::Early,
        );
        assert!(should_publish_open_position_pool_pin_after_confirmed_buy(
            &intent
        ));

        intent.source = "arb-strategy".to_string();
        assert!(!should_publish_open_position_pool_pin_after_confirmed_buy(
            &intent
        ));

        intent.source = "momentum-bot".to_string();
        intent.resources.output_mint = ironcrab::ipc::NATIVE_SOL_MINT.to_string();
        assert!(!should_publish_open_position_pool_pin_after_confirmed_buy(
            &intent
        ));

        intent.resources.output_mint = token_mint;
        intent.side = TradeSide::Sell;
        assert!(!should_publish_open_position_pool_pin_after_confirmed_buy(
            &intent
        ));

        intent.side = TradeSide::Buy;
        intent.resources.pools.clear();
        assert!(!should_publish_open_position_pool_pin_after_confirmed_buy(
            &intent
        ));
    }

    #[test]
    fn pumpfun_bonding_recovery_cold_path_includes_sell_all_without_purpose_liquidation() {
        let mut intent = TradeIntent::new_sell(
            "sell-all",
            "v0.1.0",
            "run-1",
            "id-sa".to_string(),
            "sell-all",
            IntentTier::Tier0,
            IntentOrigin::StrategyA,
            "Mint111111111111111111111111111111111111111".to_string(),
            6,
            ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
            1_000_000,
            0,
            200,
            TradingRegime::NotApplicable,
        );
        intent
            .metadata
            .insert("sell_all".to_string(), "true".to_string());
        intent
            .metadata
            .insert("dex".to_string(), "pumpfun".to_string());
        assert!(is_cold_path_recovery_sell(&intent));
    }

    #[test]
    fn pumpswap_recovery_cold_path_includes_sell_all_without_purpose_liquidation() {
        let mut intent = TradeIntent::new_sell(
            "sell-all",
            "v0.1.0",
            "run-1",
            "id-sa-pamm".to_string(),
            "sell-all",
            IntentTier::Tier0,
            IntentOrigin::StrategyA,
            "Mint111111111111111111111111111111111111111".to_string(),
            6,
            ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
            1_000_000,
            0,
            200,
            TradingRegime::NotApplicable,
        );
        intent
            .metadata
            .insert("sell_all".to_string(), "true".to_string());
        intent
            .metadata
            .insert("dex".to_string(), "pump_amm".to_string());
        assert!(is_cold_path_recovery_sell(&intent));
    }

    #[test]
    fn pumpfun_bonding_recovery_cold_path_includes_liquidation_and_kill_switch() {
        let mut intent = TradeIntent::new_sell(
            "execution-engine",
            "v0.1.0",
            "run-1",
            "id-liq".to_string(),
            "execution-engine",
            IntentTier::Tier0,
            IntentOrigin::ExecutionMevB,
            "Mint222222222222222222222222222222222222222".to_string(),
            6,
            ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
            1_000_000,
            0,
            200,
            TradingRegime::NotApplicable,
        );
        intent
            .metadata
            .insert("purpose".to_string(), "liquidation".to_string());
        assert!(is_cold_path_recovery_sell(&intent));

        intent.metadata.remove("purpose");
        intent
            .metadata
            .insert("kill_switch".to_string(), "true".to_string());
        assert!(is_cold_path_recovery_sell(&intent));
    }

    #[test]
    fn pumpfun_bonding_recovery_not_momentum_hot_path_sell() {
        let mut intent = TradeIntent::new_sell(
            "momentum-bot",
            "v0.1.0",
            "run-1",
            "id-mo".to_string(),
            "momentum-bot",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            "Mint333333333333333333333333333333333333333".to_string(),
            6,
            ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
            1_000_000,
            0,
            200,
            TradingRegime::Early,
        );
        intent
            .metadata
            .insert("dex".to_string(), "pumpfun".to_string());
        assert!(!is_cold_path_recovery_sell(&intent));
    }

    #[test]
    fn wait_for_pumpfun_bonding_cache_refresh_detects_jetstream_merge() {
        let mint_str = "Mint111111111111111111111111111111111111111";
        let mint = Pubkey::from_str(mint_str).unwrap();
        let (bonding_curve, _) = PumpFunDex::derive_bonding_curve_static(&mint);
        let assoc = Pubkey::new_unique();

        let cache = LivePoolCache::new();
        let before = (100u64, 200u64, 10u64, 20u64, false, false);
        cache.upsert(
            bonding_curve,
            CachedPoolState::PumpFun(PumpFunState {
                token_mint: mint,
                bonding_curve,
                associated_bonding_curve: assoc,
                virtual_sol_reserves: before.1,
                virtual_token_reserves: before.0,
                real_sol_reserves: before.3,
                real_token_reserves: before.2,
                complete: before.4,
                creator: Pubkey::new_unique(),
                cashback_enabled: before.5,
            }),
            0,
        );

        let cache_clone = std::sync::Arc::new(cache);
        let cache_for_task = std::sync::Arc::clone(&cache_clone);
        let bonding = bonding_curve;
        let mint_copy = mint;
        let assoc_copy = assoc;
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(30));
            cache_for_task.upsert(
                bonding,
                CachedPoolState::PumpFun(PumpFunState {
                    token_mint: mint_copy,
                    bonding_curve: bonding,
                    associated_bonding_curve: assoc_copy,
                    virtual_sol_reserves: before.1,
                    virtual_token_reserves: before.0 + 1,
                    real_sol_reserves: before.3,
                    real_token_reserves: before.2,
                    complete: before.4,
                    creator: Pubkey::new_unique(),
                    cashback_enabled: before.5,
                }),
                1,
            );
        });

        let rt = tokio::runtime::Runtime::new().unwrap();
        let ok = rt.block_on(wait_for_pumpfun_bonding_cache_refresh(
            cache_clone.as_ref(),
            &bonding_curve,
            Some(before),
            500,
            5,
            false,
        ));
        assert!(
            ok,
            "expected snapshot change after simulated PoolCacheUpdate merge"
        );
    }

    #[test]
    fn wait_for_pumpfun_bonding_cache_refresh_times_out_when_snapshot_unchanged() {
        let mint_str = "Mint222222222222222222222222222222222222222";
        let mint = Pubkey::from_str(mint_str).unwrap();
        let (bonding_curve, _) = PumpFunDex::derive_bonding_curve_static(&mint);
        let snap = (50u64, 60u64, 1u64, 2u64, false, true);
        let cache = LivePoolCache::new();
        cache.upsert(
            bonding_curve,
            CachedPoolState::PumpFun(PumpFunState {
                token_mint: mint,
                bonding_curve,
                associated_bonding_curve: Pubkey::new_unique(),
                virtual_sol_reserves: snap.1,
                virtual_token_reserves: snap.0,
                real_sol_reserves: snap.3,
                real_token_reserves: snap.2,
                complete: snap.4,
                creator: Pubkey::new_unique(),
                cashback_enabled: snap.5,
            }),
            0,
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        let ok = rt.block_on(wait_for_pumpfun_bonding_cache_refresh(
            &cache,
            &bonding_curve,
            Some(snap),
            80,
            10,
            false,
        ));
        assert!(!ok);
    }

    /// Bug #36 / #34: cold-path wait needs explicit Ready **and** a snapshot change vs pre-request
    /// (proves fresh JetStream merge for this EnsurePumpfunBondingCurve request).
    #[test]
    fn wait_for_pumpfun_bonding_cache_refresh_require_ready_requires_explicit_merge() {
        let mint_str = "Mint444444444444444444444444444444444444444";
        let mint = Pubkey::from_str(mint_str).unwrap();
        let (bonding_curve, _) = PumpFunDex::derive_bonding_curve_static(&mint);
        let snap = (50u64, 60u64, 1u64, 2u64, false, true);
        let cache = LivePoolCache::new();
        cache.upsert(
            bonding_curve,
            CachedPoolState::PumpFun(PumpFunState {
                token_mint: mint,
                bonding_curve,
                associated_bonding_curve: Pubkey::new_unique(),
                virtual_sol_reserves: snap.1,
                virtual_token_reserves: snap.0,
                real_sol_reserves: snap.3,
                real_token_reserves: snap.2,
                complete: snap.4,
                creator: Pubkey::new_unique(),
                cashback_enabled: snap.5,
            }),
            0,
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        let ok_no_ready = rt.block_on(wait_for_pumpfun_bonding_cache_refresh(
            &cache,
            &bonding_curve,
            Some(snap),
            60,
            10,
            true,
        ));
        assert!(
            !ok_no_ready,
            "without explicit Ready merge, must not succeed even when snapshot matches"
        );

        cache.merge_pumpfun_bonding_readiness(bonding_curve, DexPoolReadiness::Ready);
        let ok_stale_ready_same_snap = rt.block_on(wait_for_pumpfun_bonding_cache_refresh(
            &cache,
            &bonding_curve,
            Some(snap),
            80,
            10,
            true,
        ));
        assert!(
            !ok_stale_ready_same_snap,
            "pre-existing Ready + unchanged snapshot must not satisfy wait (no fresh merge proof)"
        );

        cache.upsert(
            bonding_curve,
            CachedPoolState::PumpFun(PumpFunState {
                token_mint: mint,
                bonding_curve,
                associated_bonding_curve: Pubkey::new_unique(),
                virtual_sol_reserves: snap.1,
                virtual_token_reserves: snap.0 + 1,
                real_sol_reserves: snap.3,
                real_token_reserves: snap.2,
                complete: snap.4,
                creator: Pubkey::new_unique(),
                cashback_enabled: snap.5,
            }),
            1,
        );
        let ok_fresh = rt.block_on(wait_for_pumpfun_bonding_cache_refresh(
            &cache,
            &bonding_curve,
            Some(snap),
            200,
            5,
            true,
        ));
        assert!(
            ok_fresh,
            "explicit Ready + snapshot change vs before must satisfy cold-path wait"
        );
    }

    /// I-24d: Verifies that ControlResponse status maps correctly to DiscoveryRequestOutcome.
    /// NotFound and Error must NOT be confused with Timeout (terminal outcomes are distinct).
    #[test]
    fn discovery_response_status_maps_to_outcome() {
        for status in [
            ControlResponseStatus::Ok,
            ControlResponseStatus::NotFound,
            ControlResponseStatus::Error,
        ] {
            let mut resp = ControlResponse::new(
                "market-data",
                "v0.1.0",
                "run-1",
                "req-1".to_string(),
                "market-data",
                status,
            );
            if matches!(status, ControlResponseStatus::Error) {
                resp = resp.with_message("test".to_string());
            }
            let actual = match resp.status {
                ControlResponseStatus::Ok => DiscoveryRequestOutcome::Ok,
                ControlResponseStatus::NotFound => DiscoveryRequestOutcome::NotFound,
                ControlResponseStatus::Busy => DiscoveryRequestOutcome::Timeout,
                ControlResponseStatus::Error => DiscoveryRequestOutcome::Error(
                    resp.message.unwrap_or_else(|| "unknown".to_string()),
                ),
            };
            assert!(
                !matches!(actual, DiscoveryRequestOutcome::Timeout),
                "status {:?} must map to Ok/NotFound/Error, never Timeout",
                status
            );
        }
    }

    /// Scope 35b: pre-plan gate must match tx_builder (≥12 accounts on hint pool row), not ≥14.
    #[test]
    fn pump_amm_preplan_builder_readiness_accepts_12_account_cache_row() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let wsol = Pubkey::from_str(ironcrab::ipc::NATIVE_SOL_MINT).unwrap();
        let short: Vec<Pubkey> = (0..11).map(|_| Pubkey::new_unique()).collect();
        cache.upsert(
            pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: base,
                quote_mint: wsol,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1),
                quote_reserve: Some(1),
                pool_accounts: short,
                creator: None,
            }),
            0,
        );
        assert!(
            !pump_amm_hint_pool_cache_usable_for_tx_plan_builder(&cache, &pool),
            "11 accounts must not satisfy tx_builder empty-accounts path"
        );

        let twelve: Vec<Pubkey> = (0..12).map(|_| Pubkey::new_unique()).collect();
        cache.upsert(
            pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: base,
                quote_mint: wsol,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1),
                quote_reserve: Some(1),
                pool_accounts: twelve,
                creator: None,
            }),
            0,
        );
        assert!(pump_amm_hint_pool_cache_usable_for_tx_plan_builder(
            &cache, &pool
        ));
    }

    #[test]
    fn pump_amm_preplan_wait_succeeds_after_cache_merge() {
        let cache = std::sync::Arc::new(LivePoolCache::new());
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let wsol = Pubkey::from_str(ironcrab::ipc::NATIVE_SOL_MINT).unwrap();
        let cache_clone = std::sync::Arc::clone(&cache);
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(30));
            let twelve: Vec<Pubkey> = (0..12).map(|_| Pubkey::new_unique()).collect();
            cache_clone.upsert(
                pool,
                CachedPoolState::PumpAmm(PumpAmmState {
                    base_mint: base,
                    quote_mint: wsol,
                    pool_base_token_account: Pubkey::new_unique(),
                    pool_quote_token_account: Pubkey::new_unique(),
                    base_reserve: Some(1),
                    quote_reserve: Some(1),
                    pool_accounts: twelve,
                    creator: None,
                }),
                1,
            );
        });

        let rt = tokio::runtime::Runtime::new().unwrap();
        let ok = rt.block_on(wait_for_pump_amm_pool_hint_ready_for_tx_plan_builder(
            cache.as_ref(),
            &pool,
            500,
            10,
        ));
        assert!(ok, "wait must observe ≥12 accounts after background upsert");
    }

    #[test]
    fn pump_amm_force_refresh_wait_requires_snapshot_change_vs_before() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let wsol = Pubkey::from_str(ironcrab::ipc::NATIVE_SOL_MINT).unwrap();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        cache.upsert(
            pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: base,
                quote_mint: wsol,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1_000),
                quote_reserve: Some(2_000),
                pool_accounts: accounts,
                creator: None,
            }),
            1,
        );
        cache.set_pump_amm_pool_accounts_readiness_authoritative(pool, DexPoolReadiness::Ready);
        cache.set_pump_amm_sell_layout_ready(&pool, true);

        let before = pump_amm_slave_recovery_snapshot(&cache, &pool);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ok = rt.block_on(wait_for_pump_amm_slave_after_recovery(
            &cache, &pool, before, 80, 10,
        ));
        assert!(
            !ok,
            "stale explicit-ready snapshot must not satisfy force-refresh wait"
        );
    }

    #[test]
    fn pump_amm_force_refresh_wait_is_pool_specific_not_mint_wide() {
        let cache = LivePoolCache::new();
        let base = Pubkey::new_unique();
        let wsol = Pubkey::from_str(ironcrab::ipc::NATIVE_SOL_MINT).unwrap();
        let target_pool = Pubkey::new_unique();
        let other_pool = Pubkey::new_unique();
        let accounts_target: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        let accounts_other: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            target_pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: base,
                quote_mint: wsol,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1_000),
                quote_reserve: Some(2_000),
                pool_accounts: accounts_target,
                creator: None,
            }),
            1,
        );
        cache.set_pump_amm_pool_accounts_readiness_authoritative(
            target_pool,
            DexPoolReadiness::Partial,
        );
        cache.set_pump_amm_sell_layout_ready(&target_pool, false);

        cache.upsert(
            other_pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: base,
                quote_mint: wsol,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(10_000),
                quote_reserve: Some(20_000),
                pool_accounts: accounts_other,
                creator: None,
            }),
            2,
        );
        cache.set_pump_amm_pool_accounts_readiness_authoritative(
            other_pool,
            DexPoolReadiness::Ready,
        );
        cache.set_pump_amm_sell_layout_ready(&other_pool, true);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let ok = rt.block_on(wait_for_pump_amm_slave_after_recovery(
            &cache,
            &target_pool,
            None,
            80,
            10,
        ));
        assert!(
            !ok,
            "ready state from another pool for same mint must not satisfy target-pool wait"
        );
    }

    #[test]
    fn pump_amm_force_refresh_wait_succeeds_after_fresh_extended_layout_merge() {
        let cache = std::sync::Arc::new(LivePoolCache::new());
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let wsol = Pubkey::from_str(ironcrab::ipc::NATIVE_SOL_MINT).unwrap();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        cache.upsert(
            pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: base,
                quote_mint: wsol,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1_000),
                quote_reserve: Some(2_000),
                pool_accounts: accounts,
                creator: None,
            }),
            1,
        );
        cache.set_pump_amm_pool_accounts_readiness_authoritative(pool, DexPoolReadiness::Ready);
        cache.set_pump_amm_sell_layout_ready(&pool, true);

        let before = pump_amm_slave_recovery_snapshot(cache.as_ref(), &pool);
        let cache_clone = std::sync::Arc::clone(&cache);
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(30));
            let third = Pubkey::new_unique();
            let t0 = Pubkey::new_unique();
            let t1 = Pubkey::new_unique();
            cache_clone.merge_pump_amm_sell_extended_layout(
                &pool,
                true,
                Some(third),
                Some(t0),
                Some(t1),
                None,
                None,
                false,
                false,
                None,
            );
            cache_clone.set_pump_amm_sell_layout_ready(&pool, true);
        });

        let rt = tokio::runtime::Runtime::new().unwrap();
        let ok = rt.block_on(wait_for_pump_amm_slave_after_recovery(
            cache.as_ref(),
            &pool,
            before,
            500,
            10,
        ));
        assert!(
            ok,
            "fresh explicit-ready merge with extended sell metadata must satisfy force-refresh wait"
        );
    }

    /// P184g: recovery snapshot must expose 27-account SSOT flags for degradation guard.
    #[test]
    fn pump_amm_recovery_snapshot_includes_pre_fee_and_inferred_ix_count() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let wsol = Pubkey::from_str(ironcrab::ipc::NATIVE_SOL_MINT).unwrap();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        cache.upsert(
            pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: base,
                quote_mint: wsol,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1),
                quote_reserve: Some(2),
                pool_accounts: accounts,
                creator: None,
            }),
            1,
        );
        let third = Pubkey::new_unique();
        let pre1 = Pubkey::new_unique();
        cache.merge_pump_amm_sell_extended_layout(
            &pool,
            true,
            Some(third),
            Some(Pubkey::new_unique()),
            Some(Pubkey::new_unique()),
            Some(Pubkey::new_unique()),
            None,
            false,
            true,
            Some(pre1),
        );
        let snap = pump_amm_slave_recovery_snapshot(&cache, &pool).expect("pump amm row");
        assert!(snap.9, "sell_requires_pre_fee_metas");
        assert_eq!(
            snap.10,
            ironcrab::solana::dex::pumpfun_amm::PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS as u8
        );
        assert_eq!(snap.11, Some(pre1));
    }

    #[test]
    fn pump_amm_force_refresh_wait_succeeds_on_layout_generation_bump_same_reserves() {
        let cache = std::sync::Arc::new(LivePoolCache::new());
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let wsol = Pubkey::from_str(ironcrab::ipc::NATIVE_SOL_MINT).unwrap();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        cache.upsert(
            pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: base,
                quote_mint: wsol,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1_000),
                quote_reserve: Some(2_000),
                pool_accounts: accounts,
                creator: None,
            }),
            1,
        );
        cache.set_pump_amm_pool_accounts_readiness_authoritative(pool, DexPoolReadiness::Ready);
        cache.set_pump_amm_sell_layout_ready(&pool, true);

        let before = pump_amm_slave_recovery_snapshot(cache.as_ref(), &pool);
        let cache_clone = std::sync::Arc::clone(&cache);
        let new_pre1 = Pubkey::new_unique();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(30));
            cache_clone.bump_pump_amm_layout_generation(&pool);
            cache_clone.merge_pump_amm_sell_extended_layout(
                &pool,
                true,
                Some(Pubkey::new_unique()),
                Some(Pubkey::new_unique()),
                Some(Pubkey::new_unique()),
                Some(Pubkey::new_unique()),
                None,
                false,
                true,
                Some(new_pre1),
            );
        });

        let rt = tokio::runtime::Runtime::new().unwrap();
        let ok = rt.block_on(wait_for_pump_amm_slave_after_recovery(
            cache.as_ref(),
            &pool,
            before,
            500,
            10,
        ));
        assert!(
            ok,
            "layout_generation bump with changed pre_fee must satisfy force-refresh wait even if reserves unchanged"
        );
    }

    #[test]
    fn sort_route_candidates_prefers_highest_and_keeps_first_on_tie() {
        let candidates = vec![
            RouteCandidate {
                dex: "raydium".to_string(),
                amount_out: 100,
                pool_id: "pool-1".to_string(),
                accounts: vec!["a1".to_string()],
                creator: None,
                execution_min_out_lamports: None,
            },
            RouteCandidate {
                dex: "orca".to_string(),
                amount_out: 200,
                pool_id: "pool-2".to_string(),
                accounts: vec!["a2".to_string()],
                creator: None,
                execution_min_out_lamports: None,
            },
            RouteCandidate {
                dex: "pump_amm".to_string(),
                amount_out: 200,
                pool_id: "pool-3".to_string(),
                accounts: vec!["a3".to_string()],
                creator: None,
                execution_min_out_lamports: None,
            },
        ];

        let sorted = sort_route_candidates_by_amount_out(candidates);
        let best = sorted.first().expect("expected a best route");
        assert_eq!(best.dex, "orca");
        assert_eq!(best.pool_id, "pool-2");
        assert_eq!(best.amount_out, 200);
    }

    #[test]
    fn multi_pool_fallback_take_next_updates_intent_and_drains_queue() {
        use super::BUILD_VERSION;
        use ironcrab::solana::dex_parser::SOL_MINT;

        let mut intent = TradeIntent::new_sell(
            "execution-engine",
            BUILD_VERSION,
            "run",
            "id-mp-fb".to_string(),
            "execution-engine",
            IntentTier::Tier0,
            IntentOrigin::ExecutionMevB,
            "mint".to_string(),
            6,
            SOL_MINT.to_string(),
            1_000,
            0,
            100,
            TradingRegime::NotApplicable,
        );
        intent
            .metadata
            .insert("sell_routing".to_string(), "multi_pool".to_string());
        intent
            .metadata
            .insert("dex".to_string(), "orca".to_string());
        intent.resources.pools = vec!["orca_pool".to_string()];

        let buildable = vec![
            RouteCandidate {
                dex: "orca".to_string(),
                amount_out: 200,
                pool_id: "orca_pool".to_string(),
                accounts: vec![],
                creator: None,
                execution_min_out_lamports: None,
            },
            RouteCandidate {
                dex: "pump_amm".to_string(),
                amount_out: 100,
                pool_id: "pump_pool".to_string(),
                accounts: vec!["acct".to_string()],
                creator: None,
                execution_min_out_lamports: None,
            },
        ];
        let mut md = std::collections::HashMap::new();
        liquidation_store_multi_pool_fallback_metadata(&mut md, &buildable);
        intent.metadata.extend(md);

        assert!(take_next_multi_pool_buildable_fallback_route(&mut intent));
        assert_eq!(
            intent.metadata.get("dex").map(String::as_str),
            Some("pump_amm")
        );
        assert_eq!(intent.resources.pools, vec!["pump_pool".to_string()]);
        assert_eq!(intent.resources.accounts, vec!["acct".to_string()]);
        let min = intent
            .execution
            .as_ref()
            .and_then(|e| e.min_out.as_ref())
            .expect("min_out");
        assert_eq!(min.decimals, 9);
        assert_eq!(min.raw, 99);

        assert!(!take_next_multi_pool_buildable_fallback_route(&mut intent));
    }

    #[test]
    fn pump_amm_pool_hint_prefers_intent_pool_over_cache() {
        let from_intent = Pubkey::new_unique();
        let from_cache = Pubkey::new_unique();
        assert_eq!(
            pump_amm_pool_market_hint_merge(Some(from_intent), Some(from_cache)),
            Some(from_intent)
        );
        assert_eq!(
            pump_amm_pool_market_hint_merge(None, Some(from_cache)),
            Some(from_cache)
        );
        assert_eq!(
            pump_amm_pool_market_hint_merge(Some(from_intent), None),
            Some(from_intent)
        );
    }

    #[test]
    fn pump_amm_hot_path_refresh_cooldown_suppresses_repeat_then_allows_after_window() {
        let map = ParkingMutex::new(HashMap::new());
        let mint = Pubkey::new_unique();
        let t0 = Instant::now();
        assert!(matches!(
            try_pump_amm_hot_path_refresh_publish(&map, mint, t0),
            PumpAmmHotPathRefreshDecision::Publish
        ));
        // Cooldown starts only after a successful NATS publish (simulated here).
        record_pump_amm_hot_path_refresh_after_success(&map, mint, t0);
        let r = try_pump_amm_hot_path_refresh_publish(
            &map,
            mint,
            t0 + std::time::Duration::from_millis(10),
        );
        match r {
            PumpAmmHotPathRefreshDecision::Suppress { age, remaining } => {
                assert!(age <= std::time::Duration::from_millis(10));
                assert!(remaining > std::time::Duration::ZERO);
            }
            _ => panic!("expected suppress"),
        }
        let t_after = t0 + PUMP_AMM_HOT_PATH_REFRESH_COOLDOWN + std::time::Duration::from_millis(1);
        assert!(matches!(
            try_pump_amm_hot_path_refresh_publish(&map, mint, t_after),
            PumpAmmHotPathRefreshDecision::Publish
        ));
    }

    /// Without a successful publish, repeated try must not suppress (NATS drop must not start cooldown).
    #[test]
    fn pump_amm_hot_path_refresh_no_cooldown_without_successful_publish() {
        let map = ParkingMutex::new(HashMap::new());
        let mint = Pubkey::new_unique();
        let t0 = Instant::now();
        assert!(matches!(
            try_pump_amm_hot_path_refresh_publish(&map, mint, t0),
            PumpAmmHotPathRefreshDecision::Publish
        ));
        assert!(matches!(
            try_pump_amm_hot_path_refresh_publish(
                &map,
                mint,
                t0 + std::time::Duration::from_millis(10)
            ),
            PumpAmmHotPathRefreshDecision::Publish
        ));
    }

    #[test]
    fn pump_amm_hot_path_refresh_cooldown_is_per_base_mint() {
        let map = ParkingMutex::new(HashMap::new());
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let t0 = Instant::now();
        assert!(matches!(
            try_pump_amm_hot_path_refresh_publish(&map, mint_a, t0),
            PumpAmmHotPathRefreshDecision::Publish
        ));
        record_pump_amm_hot_path_refresh_after_success(&map, mint_a, t0);
        assert!(matches!(
            try_pump_amm_hot_path_refresh_publish(&map, mint_b, t0),
            PumpAmmHotPathRefreshDecision::Publish
        ));
    }

    /// Regression: `before` for DLMM recovery wait must be captured **before** awaiting
    /// `ControlResponse`, or a fast JetStream merge makes `before == after` and the wait times out
    /// (Bug #34 / #36 — same ordering contract as Orca).
    #[tokio::test]
    async fn meteora_dlmm_recovery_wait_needs_pre_request_evidence_snapshot() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(ironcrab::ipc::NATIVE_SOL_MINT).unwrap();

        let initial = MeteoraState {
            token_x_mint: base_mint,
            token_y_mint: quote_mint,
            reserve_x: Pubkey::new_unique(),
            reserve_y: Pubkey::new_unique(),
            active_id: 0,
            bin_step: 10,
            reserve_x_balance: Some(100),
            reserve_y_balance: Some(200),
        };
        cache.upsert(pool, CachedPoolState::Meteora(initial.clone()), 0);
        cache.merge_meteora_dlmm_pool_readiness(pool, DexPoolReadiness::Partial);

        let before_evidence = cache.get(&pool).and_then(|st| match st {
            CachedPoolState::Meteora(s) => Some((
                s.active_id,
                s.bin_step,
                s.reserve_x_balance,
                s.reserve_y_balance,
            )),
            _ => None,
        });
        assert_eq!(before_evidence, Some((0, 10, Some(100), Some(200))));

        let mut promoted = initial;
        promoted.active_id = 42;
        cache.upsert(pool, CachedPoolState::Meteora(promoted), 0);
        cache.merge_meteora_dlmm_pool_readiness(pool, DexPoolReadiness::Ready);

        let ok =
            wait_for_meteora_dlmm_slave_after_recovery(&cache, &pool, before_evidence, 2_000, 5)
                .await;
        assert!(
            ok,
            "wait should succeed when before is pre-merge and SLAVE shows Ready + changed tuple"
        );

        // If `before` were wrongly taken after the merge, evidence matches current row → no progress.
        let stale_before = cache.get(&pool).and_then(|st| match st {
            CachedPoolState::Meteora(s) => Some((
                s.active_id,
                s.bin_step,
                s.reserve_x_balance,
                s.reserve_y_balance,
            )),
            _ => None,
        });
        let stuck =
            wait_for_meteora_dlmm_slave_after_recovery(&cache, &pool, stale_before, 80, 5).await;
        assert!(
            !stuck,
            "with before == post-recovery evidence, wait must not succeed (guards Bug #34 ordering)"
        );
    }

    /// Production Scope 48 failure: `close_token_ata=true` + complete fills + probe-only sold amount
    /// must NOT be treated as full close for regular Momentum SELL.
    #[test]
    fn scope48_regular_momentum_partial_sell_close_token_ata_complete_fills_still_partial() {
        let probe = 25_313_868_645u64;
        let scale_in = 56_323_355_801u64;
        let total_pos = probe.saturating_add(scale_in);

        let s48 = scope48_confirmed_sell_close_decision(
            false, probe, total_pos,
            false, // would have been wrongly inferred from close_ata + Complete before fix
        );
        assert!(!s48.full_close);
        assert!(!s48.sell_token_account_closed);
        assert!(!s48.sell_untracked_ata);
    }

    #[test]
    fn scope48_regular_momentum_full_sell_when_sold_covers_position() {
        let total = 81_637_224_446u64;
        let s48 = scope48_confirmed_sell_close_decision(false, total, total, false);
        assert!(s48.full_close);
        assert!(!s48.sell_token_account_closed);
        assert!(s48.sell_untracked_ata);
    }

    #[test]
    fn scope48_hard_meta_token_balance_gone_sets_sell_token_account_closed() {
        let s48 = scope48_confirmed_sell_close_decision(false, 1, 1_000_000, true);
        assert!(s48.full_close);
        assert!(s48.sell_token_account_closed);
        assert!(!s48.sell_untracked_ata);
    }

    #[test]
    fn scope48_liquidation_conservative_full_close_even_if_amounts_ambiguous() {
        let s48 = scope48_confirmed_sell_close_decision(true, 1, 10_000_000, false);
        assert!(s48.full_close);
        assert!(!s48.sell_token_account_closed);
        assert!(s48.sell_untracked_ata);
    }

    /// LockManager: after partial probe sell, residual scale_in remains; Scope 47 release does not resurrect probe.
    #[test]
    fn scope48_lockmanager_probe_scale_partial_sell_residual() {
        const M: &str = "ProdMintScope48";
        let probe = 25_313_868_645u64;
        let scale_in = 56_323_355_801u64;
        let total = probe.saturating_add(scale_in);

        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([(M.to_string(), total)]));

        let mut sell = HashMap::new();
        sell.insert(M.to_string(), probe);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-probe"), 0, sell),
            LockResult::Acquired
        ));
        assert_eq!(m.available_token_balance(M), scale_in);

        let total_pos = m
            .intent_token_position_total_at_lock("sell-probe", M)
            .expect("snapshot at lock");
        let sold_raw = probe;
        let s48 = scope48_confirmed_sell_close_decision(false, sold_raw, total_pos, false);
        assert!(!s48.full_close);

        m.release_locks_after_confirmed_sell("sell-probe");
        assert_eq!(m.available_token_balance(M), scale_in);
        assert_eq!(m.count_non_zero_token_balances(), 1);
    }

    #[test]
    fn scope48_lockmanager_full_sell_zeros_balance() {
        const M: &str = "FullMintScope48";
        let total = 1_000_000u64;
        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([(M.to_string(), total)]));

        let mut sell = HashMap::new();
        sell.insert(M.to_string(), total);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-all"), 0, sell),
            LockResult::Acquired
        ));

        let total_pos = m
            .intent_token_position_total_at_lock("sell-all", M)
            .expect("snapshot at lock");
        let s48 = scope48_confirmed_sell_close_decision(false, total, total_pos, false);
        assert!(s48.full_close);

        m.set_available_token_balance(M.to_string(), 0);
        m.release_locks_after_confirmed_sell("sell-all");
        assert_eq!(m.available_token_balance(M), 0);
        assert_eq!(m.count_non_zero_token_balances(), 0);
    }

    /// No tx-meta fill (`confirmed_sell_fill_in_raw` None): still classify partial from
    /// `required_capital` + lock snapshot (production RPC/fill gap).
    #[test]
    fn scope48_apply_metadata_partial_momentum_sell_without_fill_in_raw() {
        const MINT: &str = "Scope48MintNoFill";
        let probe = 25_313_868_645u64;
        let scale_in = 56_323_355_801u64;
        let total = probe.saturating_add(scale_in);

        let lm = LockManager::new(0);
        lm.update_balances(0, HashMap::from([(MINT.to_string(), total)]));
        let mut sell_map = HashMap::new();
        sell_map.insert(MINT.to_string(), probe);
        assert!(matches!(
            lm.try_lock_capital(LockHolder::new("intent-no-fill"), 0, sell_map),
            LockResult::Acquired
        ));

        let mut intent = TradeIntent::new(
            "momentum-bot",
            "v0.1.0",
            "run-1",
            "intent-no-fill".to_string(),
            "momentum-bot",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(probe, 6),
            TradeResources {
                input_mint: MINT.to_string(),
                output_mint: ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
                pools: vec!["pool".to_string()],
                accounts: vec![],
                token_program: None,
            },
            0,
            200,
            TradeSide::Sell,
            TradingRegime::Early,
        );
        intent
            .metadata
            .insert("close_token_ata".to_string(), "true".to_string());

        let mut exec = ExecutionResult::new_sent(
            "execution-engine",
            "test",
            "run",
            "ex1".to_string(),
            "d1".to_string(),
            "intent-no-fill".to_string(),
            "momentum-bot".to_string(),
            Some(MINT.to_string()),
            Some("sig".to_string()),
            None,
        );
        exec.status = ExecutionStatus::Confirmed;

        apply_scope48_confirmed_sell_execution_metadata(&mut exec, &lm, &intent, None, false);

        assert_eq!(
            exec.metadata
                .get("sell_position_delta_applied")
                .map(|s| s.as_str()),
            Some("partial")
        );
        assert!(!exec.metadata.contains_key("sell_untracked_ata"));
        assert!(!exec.metadata.contains_key("sell_token_account_closed"));

        let sold_raw = intent.required_capital.raw;
        let total_pos = lm
            .intent_token_position_total_at_lock(&intent.intent_id, MINT)
            .expect("snapshot at lock");
        let s48 = scope48_confirmed_sell_close_decision(false, sold_raw, total_pos, false);
        assert!(!s48.full_close);
    }

    #[test]
    fn scope48_sell_all_conservative_full_close_despite_probe_vs_total() {
        const MINT: &str = "SellAllMint";
        let probe = 100u64;
        let scale = 900u64;
        let total = probe.saturating_add(scale);

        let lm = LockManager::new(0);
        lm.update_balances(0, HashMap::from([(MINT.to_string(), total)]));
        let mut sell_map = HashMap::new();
        sell_map.insert(MINT.to_string(), probe);
        assert!(matches!(
            lm.try_lock_capital(LockHolder::new("sell-all-intent"), 0, sell_map),
            LockResult::Acquired
        ));

        let mut intent = TradeIntent::new(
            "sell-all",
            "v0.1.0",
            "run-1",
            "sell-all-intent".to_string(),
            "sell-all",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(probe, 6),
            TradeResources {
                input_mint: MINT.to_string(),
                output_mint: ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
                pools: vec!["pool".to_string()],
                accounts: vec![],
                token_program: None,
            },
            0,
            200,
            TradeSide::Sell,
            TradingRegime::Early,
        );
        intent
            .metadata
            .insert("sell_all".to_string(), "true".to_string());
        assert!(is_cold_path_recovery_sell(&intent));

        let mut exec = ExecutionResult::new_sent(
            "execution-engine",
            "test",
            "run",
            "ex-sa".to_string(),
            "d-sa".to_string(),
            "sell-all-intent".to_string(),
            "sell-all".to_string(),
            Some(MINT.to_string()),
            None,
            None,
        );
        exec.status = ExecutionStatus::Confirmed;

        apply_scope48_confirmed_sell_execution_metadata(&mut exec, &lm, &intent, None, false);
        assert_eq!(
            exec.metadata
                .get("sell_position_delta_applied")
                .map(|s| s.as_str()),
            Some("full")
        );
        assert_eq!(
            exec.metadata.get("sell_untracked_ata").map(|s| s.as_str()),
            Some("true")
        );
    }

    /// PR3.1: Confirm may arrive on the 100ms poll before post-send waiter registration.
    #[tokio::test]
    async fn orphan_confirm_dispatch_then_register_yields_confirmed() {
        use super::{
            build_replay_context, confirm_via_jetstream, dispatch_wallet_tx_confirmed,
            register_wallet_tx_confirm_waiter, ConfirmOutcome,
        };
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("orphan_confirm_decisions.jsonl");
        let ctx = build_replay_context(&path, None).await.expect("replay ctx");
        let sig = "OrphanRaceSigBase58Test123";

        dispatch_wallet_tx_confirmed(
            &ctx.pending_tx_confirms,
            &ctx.recent_orphan_tx_confirms,
            sig,
            42,
            None,
        );
        assert!(
            ctx.recent_orphan_tx_confirms.read().contains_key(sig),
            "confirm should be buffered as orphan"
        );

        let pre_rx = register_wallet_tx_confirm_waiter(&ctx, sig);
        assert!(
            ctx.recent_orphan_tx_confirms.read().is_empty(),
            "orphan entry consumed on register"
        );

        let config = ctx.config.read().clone();
        let outcome = confirm_via_jetstream(
            &ctx,
            sig,
            Duration::from_secs(1),
            Instant::now(),
            &config,
            Some(pre_rx),
            Some(40),
        )
        .await
        .expect("confirm_via_jetstream");

        match outcome {
            ConfirmOutcome::Confirmed { slot, .. } => assert_eq!(slot, Some(42)),
            other => panic!("expected Confirmed, got {other:?}"),
        }
    }

    #[test]
    fn priority_fee_static_floor_when_dynamic_below_config() {
        use super::{select_priority_fee_for_intent, PriorityFeeSelection};
        use ironcrab::ipc::{
            ExplicitAmount, FeePolicy, IntentOrigin, IntentTier, PriorityFeePercentiles,
            TradeIntent, TradeResources, TradeSide, TradingRegime,
        };

        let intent = TradeIntent::new(
            "test",
            "v0.1.0",
            "run",
            "id-1".to_string(),
            "momentum-bot",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(1_000_000, 9),
            TradeResources {
                input_mint: ironcrab::ipc::NATIVE_SOL_MINT.to_string(),
                output_mint: "Mint111111111111111111111111111111111111111".to_string(),
                pools: vec!["Pool123".to_string()],
                accounts: vec![],
                token_program: None,
            },
            0,
            200,
            TradeSide::Buy,
            TradingRegime::Early,
        );
        let fee_policy = FeePolicy {
            default_priority_fee_micro_lamports: 100_000,
            ..Default::default()
        };

        let percentiles = PriorityFeePercentiles::new(
            "test", "test", "run", 50, 100, 10_000, 50_000, 90_000, 120_000, 120_000, 60_000,
            70_000,
        );
        let sel = select_priority_fee_for_intent(&intent, &fee_policy, Some(&percentiles));
        assert_eq!(
            sel,
            PriorityFeeSelection {
                fee_micro_lamports: 100_000,
                source: "static_floor",
            }
        );

        let fee_policy_dynamic = FeePolicy {
            default_priority_fee_micro_lamports: 100_000,
            tier1_fee_percentile: 75,
            tier1_fee_multiplier: 1.2,
            ..Default::default()
        };
        let sel_dynamic =
            select_priority_fee_for_intent(&intent, &fee_policy_dynamic, Some(&percentiles));
        assert_eq!(sel_dynamic.source, "dynamic");
        assert!(sel_dynamic.fee_micro_lamports > 100_000);
    }

    #[test]
    fn confirmed_slot_delta_saturating_sub_edge_cases() {
        use super::confirmed_slot_delta_slots;
        assert_eq!(confirmed_slot_delta_slots(0, 99), 0);
        assert_eq!(confirmed_slot_delta_slots(100, 102), 2);
        assert_eq!(confirmed_slot_delta_slots(200, 100), 0);
    }

    #[test]
    fn rpc_blockhash_cache_fallback_pairs_signing_slot_with_hash() {
        use super::{apply_rpc_blockhash_cache_fallback, CachedBlockhash};
        use solana_sdk::hash::Hash;
        use std::time::Instant;

        let signing_hash = Hash::new_from_array([7u8; 32]);
        let stale_hash = Hash::new_from_array([1u8; 32]);
        let mut cache = Some(CachedBlockhash {
            hash: stale_hash,
            slot: 10,
            block_height: 0,
            received_at: Instant::now(),
        });
        let now = Instant::now();
        apply_rpc_blockhash_cache_fallback(&mut cache, signing_hash, 42, now);
        let updated = cache.expect("cache updated");
        assert_eq!(updated.hash, signing_hash);
        assert_eq!(updated.slot, 42);
        assert_ne!(updated.slot, 10);
    }

    #[test]
    fn orphan_confirm_ttl_eviction_removes_stale_entries() {
        use super::{evict_stale_orphan_tx_confirms, OrphanTxConfirmEntry, WalletTxConfirmNotify};
        use parking_lot::RwLock;
        use std::collections::HashMap;
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let orphan_buffer: Arc<RwLock<HashMap<String, OrphanTxConfirmEntry>>> =
            Arc::new(RwLock::new(HashMap::new()));
        orphan_buffer.write().insert(
            "stale_sig".to_string(),
            OrphanTxConfirmEntry {
                notify: WalletTxConfirmNotify { slot: 1, err: None },
                buffered_at: Instant::now() - Duration::from_secs(300),
            },
        );

        evict_stale_orphan_tx_confirms(&orphan_buffer, Duration::from_secs(120));
        assert!(orphan_buffer.read().is_empty());
    }

    #[tokio::test]
    async fn normal_confirm_path_waiter_before_dispatch() {
        use super::{dispatch_wallet_tx_confirmed, WalletTxConfirmNotify};
        use parking_lot::RwLock;
        use std::collections::HashMap;
        use std::sync::Arc;
        use tokio::sync::oneshot;

        let pending: Arc<RwLock<HashMap<String, oneshot::Sender<WalletTxConfirmNotify>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let orphan_buffer: Arc<RwLock<HashMap<String, super::OrphanTxConfirmEntry>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let (tx, rx) = oneshot::channel();
        pending.write().insert("normal_sig".to_string(), tx);

        dispatch_wallet_tx_confirmed(&pending, &orphan_buffer, "normal_sig", 77, None);

        assert!(orphan_buffer.read().is_empty());
        let confirm = rx.await.expect("waiter should receive confirm");
        assert_eq!(confirm.slot, 77);
        assert!(confirm.err.is_none());
    }

    /// Phase-1 hot-path isolation: intent dispatch must not wait on main-loop Pool/Wallet heartbeat work.
    #[tokio::test]
    async fn intent_dispatcher_not_starved_by_simulated_main_loop_block() {
        use super::{build_replay_context, create_test_intent, run_intent_dispatcher};
        use ironcrab::metrics::inc_execution_intent_rx_queue_depth;
        use std::sync::atomic::Ordering;
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("intent_dispatch_decisions.jsonl");
        let ctx = Arc::new(build_replay_context(&path, None).await.expect("replay ctx"));
        let (intent_tx, intent_rx) = tokio::sync::mpsc::channel::<ironcrab::ipc::TradeIntent>(100);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let enqueue_tracker = super::IntentChannelEnqueueTracker::new();

        let dispatcher = tokio::spawn(run_intent_dispatcher(
            intent_rx,
            Arc::clone(&ctx),
            enqueue_tracker,
            shutdown_rx,
        ));

        // Simulate main-loop heartbeat stuck in Pool/Wallet batch processing.
        let main_loop_blocker = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let intent = create_test_intent("intent-dispatch-test");
        let send_started = Instant::now();
        intent_tx.send(intent).await.expect("enqueue intent");
        inc_execution_intent_rx_queue_depth();

        let deadline = tokio::time::sleep(Duration::from_millis(100));
        tokio::pin!(deadline);
        loop {
            if ctx.intents_received.load(Ordering::Relaxed) >= 1 {
                break;
            }
            tokio::select! {
                _ = &mut deadline => {
                    panic!(
                        "intent not dispatched within 100ms while main loop blocked for 5s (elapsed {:?})",
                        send_started.elapsed()
                    );
                }
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }

        assert!(
            send_started.elapsed() < Duration::from_millis(100),
            "dispatch latency {:?} exceeded 100ms target",
            send_started.elapsed()
        );

        shutdown_tx.send(true).expect("signal shutdown");
        dispatcher.await.expect("dispatcher join");
        main_loop_blocker.abort();
    }
}
