//! Cache-scoped cold-path RPC refresh helpers (I-24d).

use super::host::ColdHost;
use crate::execution::live_pool_cache::{
    meteora_cpmm_readiness_for_pool_cache_update, meteora_dlmm_readiness_for_pool_cache_update,
    orca_readiness_for_pool_cache_update, parse_pool_account,
    raydium_amm_readiness_for_pool_cache_update, raydium_amm_serum_needs_cold_backfill,
    raydium_amm_serum_static_accounts_ready, CachedPoolState, MeteoraCpmmState, MeteoraState,
    OrcaWhirlpoolState, RaydiumAmmState, RaydiumCpmmState,
};
use crate::ipc::{
    DexPoolReadiness, PoolCacheUpdate, NATIVE_SOL_MINT,
    POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY, POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY,
    POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY, POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY,
    POOL_CACHE_UPDATE_METEORA_DLMM_ONCHAIN_MINTS_KEY, POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY,
    POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY, POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY,
    POOL_CACHE_UPDATE_ORCA_ONCHAIN_MINTS_KEY, POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY,
    POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY, POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY,
    POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY, POOL_CACHE_UPDATE_ORCA_TOKEN_A_PROGRAM_KEY,
    POOL_CACHE_UPDATE_ORCA_TOKEN_B_PROGRAM_KEY, POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY,
    POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY,
};
use crate::nats::jetstream::pool_subject;
use crate::solana::dex::raydium::Raydium;
use crate::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const METEORA_DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const METEORA_CPMM: &str = "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D";

fn try_parse_token_account_balance(data: &[u8]) -> Option<u64> {
    use spl_token::solana_program::program_pack::Pack;
    spl_token::state::Account::unpack(data)
        .ok()
        .map(|a| a.amount)
}

fn raydium_cpmm_vaults_for_pool_cache_update(s: &RaydiumCpmmState) -> String {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
    let (first_vault, second_vault) = if s.token_1_mint == sol {
        (s.token_0_vault, s.token_1_vault)
    } else if s.token_0_mint == sol {
        (s.token_1_vault, s.token_0_vault)
    } else {
        (s.token_0_vault, s.token_1_vault)
    };
    format!("{first_vault},{second_vault}")
}

/// Meteora CPMM: same normalized base/quote vault ordering as Raydium CPMM for JetStream metadata.
fn meteora_cpmm_vaults_for_pool_cache_update(s: &MeteoraCpmmState) -> String {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
    let (first_vault, second_vault) = if s.token_1_mint == sol {
        (s.token_0_vault, s.token_1_vault)
    } else if s.token_0_mint == sol {
        (s.token_1_vault, s.token_0_vault)
    } else {
        (s.token_0_vault, s.token_1_vault)
    };
    format!("{first_vault},{second_vault}")
}

/// On-chain `token_0_mint,token_1_mint` for SLAVE bootstrap when JetStream uses normalized base/quote.
fn meteora_cpmm_onchain_mints_for_pool_cache_update(s: &MeteoraCpmmState) -> String {
    format!("{},{}", s.token_0_mint, s.token_1_mint)
}

/// On-chain `token_mint_a,token_mint_b` for SLAVE bootstrap when JetStream uses normalized base/quote.
fn orca_onchain_mints_for_pool_cache_update(s: &OrcaWhirlpoolState) -> String {
    format!("{},{}", s.token_mint_a, s.token_mint_b)
}

/// Orca Whirlpool: PoolCacheUpdate metadata keys for SLAVE bootstrap (BalanceUpdated + PoolDiscovered).
fn orca_metadata_for_pool_cache_update(s: &OrcaWhirlpoolState) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY.to_string(),
        format!("{},{}", s.token_vault_a, s.token_vault_b),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_ONCHAIN_MINTS_KEY.to_string(),
        orca_onchain_mints_for_pool_cache_update(s),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY.to_string(),
        s.tick_current_index.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY.to_string(),
        s.tick_spacing.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY.to_string(),
        s.sqrt_price.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY.to_string(),
        s.liquidity.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY.to_string(),
        s.fee_rate.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY.to_string(),
        s.protocol_fee_rate.to_string(),
    );
    if let Some(p) = s.token_a_program {
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_TOKEN_A_PROGRAM_KEY.to_string(),
            p.to_string(),
        );
    }
    if let Some(p) = s.token_b_program {
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_TOKEN_B_PROGRAM_KEY.to_string(),
            p.to_string(),
        );
    }
    meta
}

/// On-chain `token_x_mint,token_y_mint` for SLAVE bootstrap when JetStream uses normalized base/quote.
fn meteora_dlmm_onchain_mints_for_pool_cache_update(s: &MeteoraState) -> String {
    format!("{},{}", s.token_x_mint, s.token_y_mint)
}

/// Meteora DLMM: vault pubkeys + static pool fields for JetStream / SLAVE bootstrap.
fn meteora_dlmm_metadata_for_pool_cache_update(s: &MeteoraState) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert(
        POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY.to_string(),
        format!("{},{}", s.reserve_x, s.reserve_y),
    );
    meta.insert(
        POOL_CACHE_UPDATE_METEORA_DLMM_ONCHAIN_MINTS_KEY.to_string(),
        meteora_dlmm_onchain_mints_for_pool_cache_update(s),
    );
    meta.insert(
        POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY.to_string(),
        s.active_id.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY.to_string(),
        s.bin_step.to_string(),
    );
    meta
}

/// JetStream readiness for Raydium CPMM (SOL-aware base/quote): single source for BalanceUpdated and PoolDiscovered.
fn raydium_cpmm_readiness_for_pool_cache_update(s: &RaydiumCpmmState) -> DexPoolReadiness {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
    let r0 = s.reserve_0.unwrap_or(0);
    let r1 = s.reserve_1.unwrap_or(0);
    let (base_side_liq, quote_side_liq) = if s.token_1_mint == sol {
        (r0 > 0, r1 > 0)
    } else if s.token_0_mint == sol {
        (r1 > 0, r0 > 0)
    } else {
        (r0 > 0, r1 > 0)
    };
    if base_side_liq && quote_side_liq {
        DexPoolReadiness::Ready
    } else if r0 > 0 || r1 > 0 {
        DexPoolReadiness::Partial
    } else {
        DexPoolReadiness::Observed
    }
}

/// Result of one cache-scoped Meteora CPMM RPC refresh (pool + vault balances).
pub struct MeteoraCpmmRpcRefreshRowResult {
    pub pool_addr: Pubkey,
    pub readiness: DexPoolReadiness,
    /// `true` when a non-`Observed` [`DexPoolReadiness`] update was published to JetStream.
    pub jetstream_published: bool,
}

/// Cold-path only: refresh one Meteora CPMM row already present in MASTER cache — no global scan.
pub async fn cold_path_rpc_refresh_meteora_cpmm_pool_row(
    host: &impl ColdHost,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint: &Pubkey,
    pool_addr: Pubkey,
    mut state: MeteoraCpmmState,
) -> Option<MeteoraCpmmRpcRefreshRowResult> {
    let meteora_cpmm_program = Pubkey::from_str(METEORA_CPMM).expect("METEORA_CPMM constant");
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();

    let pool_acc = match rpc.get_account_opt_retry(&pool_addr).await {
        Ok(Some(acc)) => acc,
        Ok(None) | Err(_) => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                "Meteora CPMM RPC refresh: pool account fetch miss, skip"
            );
            return None;
        }
    };

    let parsed_state = match parse_pool_account(&meteora_cpmm_program, &pool_acc.data) {
        Some(CachedPoolState::MeteoraCpmm(s)) => s,
        _ => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                data_len = pool_acc.data.len(),
                "Meteora CPMM RPC refresh: parse pool failed, skip"
            );
            return None;
        }
    };

    state.token_0_mint = parsed_state.token_0_mint;
    state.token_1_mint = parsed_state.token_1_mint;
    state.token_0_vault = parsed_state.token_0_vault;
    state.token_1_vault = parsed_state.token_1_vault;
    state.amm_config = parsed_state.amm_config;
    state.observation_key = parsed_state.observation_key;
    state.token_0_program = parsed_state.token_0_program;
    state.token_1_program = parsed_state.token_1_program;
    state.mint_0_decimals = parsed_state.mint_0_decimals;
    state.mint_1_decimals = parsed_state.mint_1_decimals;
    state.status = parsed_state.status;

    if state.token_0_mint != *base_mint && state.token_1_mint != *base_mint {
        return None;
    }

    let vault0 = state.token_0_vault;
    let vault1 = state.token_1_vault;
    let bal0 = match rpc.get_account_opt_retry(&vault0).await {
        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
        _ => None,
    };
    let bal1 = match rpc.get_account_opt_retry(&vault1).await {
        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
        _ => None,
    };
    let (b0, b1) = match (bal0, bal1) {
        (Some(a), Some(b)) => (a, b),
        _ => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                "Meteora CPMM RPC refresh: vault balance RPC incomplete, skip"
            );
            return None;
        }
    };

    state.reserve_0 = b0;
    state.reserve_1 = b1;

    host.live_pool_cache()
        .upsert(pool_addr, CachedPoolState::MeteoraCpmm(state.clone()), 0);

    let readiness = meteora_cpmm_readiness_for_pool_cache_update(&state);
    host.live_pool_cache()
        .merge_meteora_cpmm_pool_readiness(pool_addr, readiness);

    if readiness == DexPoolReadiness::Observed {
        debug!(
            request_id = %request_id,
            pool = %pool_addr,
            mint = %base_mint,
            "Meteora CPMM RPC refresh: reserves still degenerate (Observed), no JetStream publish"
        );
        return Some(MeteoraCpmmRpcRefreshRowResult {
            pool_addr,
            readiness,
            jetstream_published: false,
        });
    }

    let (pub_base_mint, pub_quote_mint, base_r, quote_r) = if state.token_1_mint == sol {
        (state.token_0_mint, state.token_1_mint, b0, b1)
    } else if state.token_0_mint == sol {
        (state.token_1_mint, state.token_0_mint, b1, b0)
    } else {
        (state.token_0_mint, state.token_1_mint, b0, b1)
    };

    let jetstream_ok = if let Some(nats) = host.nats() {
        let mut balance_update = PoolCacheUpdate::new_balance_updated(
            "market-data",
            BUILD_VERSION,
            run_id,
            pool_addr.to_string(),
            "meteora_cpmm".to_string(),
            pub_base_mint.to_string(),
            pub_quote_mint.to_string(),
            base_r,
            quote_r,
            0,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY.to_string(),
            meteora_cpmm_vaults_for_pool_cache_update(&state),
        );
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY.to_string(),
            meteora_cpmm_onchain_mints_for_pool_cache_update(&state),
        );
        balance_update.metadata = Some(meta);
        balance_update.set_dex_readiness_in_metadata(readiness);
        let subject = pool_subject(&pool_addr.to_string());
        match nats.jetstream_publish(&subject, &balance_update).await {
            Ok(true) => {
                info!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    mint = %base_mint,
                    readiness = ?readiness,
                    "Meteora CPMM RPC refresh: published PoolCacheUpdate::BalanceUpdated to JetStream"
                );
                true
            }
            Ok(false) => {
                warn!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    "Meteora CPMM RPC refresh: JetStream publish failed (timeout or drop)"
                );
                false
            }
            Err(e) => {
                warn!(
                    error = %e,
                    request_id = %request_id,
                    "Meteora CPMM RPC refresh: Failed to publish PoolCacheUpdate to JetStream"
                );
                false
            }
        }
    } else {
        false
    };

    if !jetstream_ok {
        warn!(
            request_id = %request_id,
            pool = %pool_addr,
            "Meteora CPMM RPC refresh: MASTER cache updated but JetStream publish failed — SSOT drift risk"
        );
    }

    Some(MeteoraCpmmRpcRefreshRowResult {
        pool_addr,
        readiness,
        jetstream_published: jetstream_ok,
    })
}

/// Result of one cache-scoped Orca Whirlpool RPC refresh (pool account + vault balances).
pub struct OrcaWhirlpoolRpcRefreshRowResult {
    pub pool_addr: Pubkey,
    pub readiness: DexPoolReadiness,
    /// `true` when a non-`Observed` [`DexPoolReadiness`] update was published to JetStream.
    pub jetstream_published: bool,
}

/// Cold-path only: refresh one Orca Whirlpool row already present in MASTER cache — same RPC
/// pattern as wallet bootstrap (no global scan). Updates MASTER, merges readiness, publishes
/// JetStream when readiness is not `Observed`.
///
/// Returns `None` when this pool row is skipped (fetch/parse/mint mismatch/incomplete vaults).
pub async fn cold_path_rpc_refresh_orca_whirlpool_pool_row(
    host: &impl ColdHost,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint: &Pubkey,
    pool_addr: Pubkey,
    mut state: OrcaWhirlpoolState,
) -> Option<OrcaWhirlpoolRpcRefreshRowResult> {
    let orca_program = Pubkey::from_str(ORCA_WHIRLPOOL).expect("ORCA_WHIRLPOOL constant");

    let pool_acc = match rpc.get_account_opt_retry(&pool_addr).await {
        Ok(Some(acc)) => acc,
        Ok(None) | Err(_) => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                "Orca Whirlpool RPC refresh: pool account fetch miss, skip"
            );
            return None;
        }
    };

    let parsed_state = match parse_pool_account(&orca_program, &pool_acc.data) {
        Some(CachedPoolState::Orca(s)) => s,
        _ => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                data_len = pool_acc.data.len(),
                "Orca Whirlpool RPC refresh: parse pool failed, skip"
            );
            return None;
        }
    };

    state.token_mint_a = parsed_state.token_mint_a;
    state.token_mint_b = parsed_state.token_mint_b;
    state.token_vault_a = parsed_state.token_vault_a;
    state.token_vault_b = parsed_state.token_vault_b;
    state.tick_current_index = parsed_state.tick_current_index;
    state.sqrt_price = parsed_state.sqrt_price;
    state.liquidity = parsed_state.liquidity;
    state.fee_rate = parsed_state.fee_rate;
    state.protocol_fee_rate = parsed_state.protocol_fee_rate;
    state.tick_spacing = parsed_state.tick_spacing;

    if state.token_mint_a != *base_mint && state.token_mint_b != *base_mint {
        return None;
    }

    let vault_a = state.token_vault_a;
    let vault_b = state.token_vault_b;
    let bal_a = match rpc.get_account_opt_retry(&vault_a).await {
        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
        _ => None,
    };
    let bal_b = match rpc.get_account_opt_retry(&vault_b).await {
        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
        _ => None,
    };
    let (ba, bb) = match (bal_a, bal_b) {
        (Some(a), Some(b)) => (a, b),
        _ => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                "Orca Whirlpool RPC refresh: vault balance RPC incomplete, skip"
            );
            return None;
        }
    };

    state.vault_a_balance = Some(ba);
    state.vault_b_balance = Some(bb);

    host.live_pool_cache()
        .upsert(pool_addr, CachedPoolState::Orca(state.clone()), 0);

    let readiness = orca_readiness_for_pool_cache_update(&state);
    host.live_pool_cache()
        .merge_orca_pool_readiness(pool_addr, readiness);

    if readiness == DexPoolReadiness::Observed {
        debug!(
            request_id = %request_id,
            pool = %pool_addr,
            mint = %base_mint,
            "Orca Whirlpool RPC refresh: still Observed after RPC, no JetStream publish"
        );
        return Some(OrcaWhirlpoolRpcRefreshRowResult {
            pool_addr,
            readiness,
            jetstream_published: false,
        });
    }

    let (pub_base_mint, pub_quote_mint, base_r, quote_r) =
        (state.token_mint_a, state.token_mint_b, ba, bb);

    let jetstream_ok = if let Some(nats) = host.nats() {
        let mut balance_update = PoolCacheUpdate::new_balance_updated(
            "market-data",
            BUILD_VERSION,
            run_id,
            pool_addr.to_string(),
            "orca".to_string(),
            pub_base_mint.to_string(),
            pub_quote_mint.to_string(),
            base_r,
            quote_r,
            0,
        );
        balance_update.metadata = Some(orca_metadata_for_pool_cache_update(&state));
        balance_update.set_dex_readiness_in_metadata(readiness);
        let subject = pool_subject(&pool_addr.to_string());
        match nats.jetstream_publish(&subject, &balance_update).await {
            Ok(true) => {
                info!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    mint = %base_mint,
                    readiness = ?readiness,
                    "Orca Whirlpool RPC refresh: published PoolCacheUpdate::BalanceUpdated to JetStream"
                );
                true
            }
            Ok(false) => {
                warn!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    "Orca Whirlpool RPC refresh: JetStream publish failed (timeout or drop)"
                );
                false
            }
            Err(e) => {
                warn!(
                    error = %e,
                    request_id = %request_id,
                    "Orca Whirlpool RPC refresh: Failed to publish PoolCacheUpdate to JetStream"
                );
                false
            }
        }
    } else {
        false
    };

    if !jetstream_ok {
        warn!(
            request_id = %request_id,
            pool = %pool_addr,
            "Orca Whirlpool RPC refresh: MASTER cache updated but JetStream publish failed — SSOT drift risk"
        );
    }

    Some(OrcaWhirlpoolRpcRefreshRowResult {
        pool_addr,
        readiness,
        jetstream_published: jetstream_ok,
    })
}

/// Result of one cache-scoped Meteora DLMM RPC refresh (LB pair + vault balances).
pub struct MeteoraDlmmRpcRefreshRowResult {
    pub pool_addr: Pubkey,
    pub readiness: DexPoolReadiness,
    /// `true` when a non-`Observed` [`DexPoolReadiness`] update was published to JetStream.
    pub jetstream_published: bool,
}

/// Cold-path only: refresh one Meteora DLMM row already present in MASTER cache — same RPC
/// pattern as wallet bootstrap (no global scan). Updates MASTER, merges readiness, publishes
/// JetStream when readiness is not `Observed`.
///
/// Returns `None` when this pool row is skipped (fetch/parse/mint mismatch/incomplete vaults).
pub async fn cold_path_rpc_refresh_meteora_dlmm_pool_row(
    host: &impl ColdHost,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint: &Pubkey,
    pool_addr: Pubkey,
    mut state: MeteoraState,
) -> Option<MeteoraDlmmRpcRefreshRowResult> {
    let dlmm_program = Pubkey::from_str(METEORA_DLMM).expect("METEORA_DLMM constant");

    let pool_acc = match rpc.get_account_opt_retry(&pool_addr).await {
        Ok(Some(acc)) => acc,
        Ok(None) | Err(_) => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                "Meteora DLMM RPC refresh: pool account fetch miss, skip"
            );
            return None;
        }
    };

    let parsed_state = match parse_pool_account(&dlmm_program, &pool_acc.data) {
        Some(CachedPoolState::Meteora(s)) => s,
        _ => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                data_len = pool_acc.data.len(),
                "Meteora DLMM RPC refresh: parse pool failed, skip"
            );
            return None;
        }
    };

    state.token_x_mint = parsed_state.token_x_mint;
    state.token_y_mint = parsed_state.token_y_mint;
    state.reserve_x = parsed_state.reserve_x;
    state.reserve_y = parsed_state.reserve_y;
    state.active_id = parsed_state.active_id;
    state.bin_step = parsed_state.bin_step;

    if state.token_x_mint != *base_mint && state.token_y_mint != *base_mint {
        return None;
    }

    let vx = state.reserve_x;
    let vy = state.reserve_y;
    let bal_x = match rpc.get_account_opt_retry(&vx).await {
        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
        _ => None,
    };
    let bal_y = match rpc.get_account_opt_retry(&vy).await {
        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
        _ => None,
    };
    let (bx, by) = match (bal_x, bal_y) {
        (Some(a), Some(b)) => (a, b),
        _ => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                "Meteora DLMM RPC refresh: vault balance RPC incomplete, skip"
            );
            return None;
        }
    };

    state.reserve_x_balance = Some(bx);
    state.reserve_y_balance = Some(by);

    host.live_pool_cache()
        .upsert(pool_addr, CachedPoolState::Meteora(state.clone()), 0);

    let readiness = meteora_dlmm_readiness_for_pool_cache_update(&state);
    host.live_pool_cache()
        .merge_meteora_dlmm_pool_readiness(pool_addr, readiness);

    if readiness == DexPoolReadiness::Observed {
        debug!(
            request_id = %request_id,
            pool = %pool_addr,
            mint = %base_mint,
            "Meteora DLMM RPC refresh: still Observed after RPC, no JetStream publish"
        );
        return Some(MeteoraDlmmRpcRefreshRowResult {
            pool_addr,
            readiness,
            jetstream_published: false,
        });
    }

    let (pub_base_mint, pub_quote_mint, base_r, quote_r) =
        (state.token_x_mint, state.token_y_mint, bx, by);

    let jetstream_ok = if let Some(nats) = host.nats() {
        let mut balance_update = PoolCacheUpdate::new_balance_updated(
            "market-data",
            BUILD_VERSION,
            run_id,
            pool_addr.to_string(),
            "meteora_dlmm".to_string(),
            pub_base_mint.to_string(),
            pub_quote_mint.to_string(),
            base_r,
            quote_r,
            0,
        );
        balance_update.metadata = Some(meteora_dlmm_metadata_for_pool_cache_update(&state));
        balance_update.set_dex_readiness_in_metadata(readiness);
        let subject = pool_subject(&pool_addr.to_string());
        match nats.jetstream_publish(&subject, &balance_update).await {
            Ok(true) => {
                info!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    mint = %base_mint,
                    readiness = ?readiness,
                    "Meteora DLMM RPC refresh: published PoolCacheUpdate::BalanceUpdated to JetStream"
                );
                true
            }
            Ok(false) => {
                warn!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    "Meteora DLMM RPC refresh: JetStream publish failed (timeout or drop)"
                );
                false
            }
            Err(e) => {
                warn!(
                    error = %e,
                    request_id = %request_id,
                    "Meteora DLMM RPC refresh: Failed to publish PoolCacheUpdate to JetStream"
                );
                false
            }
        }
    } else {
        false
    };

    if !jetstream_ok {
        warn!(
            request_id = %request_id,
            pool = %pool_addr,
            "Meteora DLMM RPC refresh: MASTER cache updated but JetStream publish failed — SSOT drift risk"
        );
    }

    Some(MeteoraDlmmRpcRefreshRowResult {
        pool_addr,
        readiness,
        jetstream_published: jetstream_ok,
    })
}

/// Cold-path only: fetch Serum/OpenBook static accounts when any field is missing (including vaults).
///
/// Returns `true` when the cache row now has the full static Serum layout required for swap building.
pub async fn cold_path_backfill_raydium_serum_accounts(
    host: &impl ColdHost,
    rpc: &Arc<SolanaRpc>,
    request_id: &str,
    pool_addr: Pubkey,
    state: &RaydiumAmmState,
) -> bool {
    if !raydium_amm_serum_needs_cold_backfill(state) {
        return raydium_amm_serum_static_accounts_ready(state);
    }

    match rpc.get_account_retry(&state.market_id).await {
        Ok(account) => {
            if let Some((bids_o, asks_o, eq_o, bv_o, qv_o)) =
                Raydium::parse_serum_market_accounts(&account.data)
            {
                if let (Some(b), Some(a), Some(e)) = (bids_o, asks_o, eq_o) {
                    host.live_pool_cache()
                        .set_raydium_serum_accounts(&pool_addr, b, a, e, bv_o, qv_o);
                    if let Some(CachedPoolState::RaydiumAmm(st)) =
                        host.live_pool_cache().get(&pool_addr)
                    {
                        if raydium_amm_serum_static_accounts_ready(&st) {
                            host.raydium_serum_fetched_insert(pool_addr);
                            return true;
                        }
                    }
                }
            }
        }
        Err(e) => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                market_id = %state.market_id,
                error = %e,
                "Raydium AMM serum backfill: market fetch failed (may stay Partial)"
            );
        }
    }

    false
}

/// FIX-29 follow-up: one-shot serum backfill from md-sidefx / Geyser cache upsert (cold path only).
///
/// Dedupes via [`ColdHost::raydium_serum_fetched_try_claim`]; publishes JetStream when layout becomes
/// usable so SLAVE caches receive serum vault pubkeys.
pub async fn cold_path_raydium_serum_backfill_and_publish(
    host: &impl ColdHost,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    pool_addr: Pubkey,
    state: &RaydiumAmmState,
) -> bool {
    if !raydium_amm_serum_needs_cold_backfill(state) {
        return false;
    }
    if !host.raydium_serum_fetched_try_claim(pool_addr) {
        return false;
    }

    let backfill_ok =
        cold_path_backfill_raydium_serum_accounts(host, rpc, request_id, pool_addr, state).await;
    if !backfill_ok {
        host.raydium_serum_fetched_remove(pool_addr);
        return false;
    }

    let readiness = match host.live_pool_cache().get(&pool_addr).as_ref() {
        Some(CachedPoolState::RaydiumAmm(st)) => raydium_amm_readiness_for_pool_cache_update(st),
        _ => DexPoolReadiness::Observed,
    };
    host.live_pool_cache()
        .merge_raydium_amm_pool_readiness(pool_addr, readiness);

    let Some(nats) = host.nats() else {
        host.raydium_serum_fetched_insert(pool_addr);
        return true;
    };
    let Some((CachedPoolState::RaydiumAmm(st), pool_slot, _age_ms)) =
        host.live_pool_cache().get_with_metadata(&pool_addr)
    else {
        return true;
    };

    let mut balance_update = PoolCacheUpdate::new_balance_updated(
        "market-data",
        BUILD_VERSION,
        run_id,
        pool_addr.to_string(),
        "raydium".to_string(),
        st.base_mint.to_string(),
        st.quote_mint.to_string(),
        st.coin_reserve.unwrap_or(0),
        st.pc_reserve.unwrap_or(0),
        pool_slot,
    );
    let mut meta = HashMap::new();
    if st.market_id != Pubkey::default() {
        meta.insert("market_id".to_string(), st.market_id.to_string());
    }
    if let (Some(bids), Some(asks), Some(eq)) = (st.serum_bids, st.serum_asks, st.serum_event_queue)
    {
        meta.insert("serum_bids".to_string(), bids.to_string());
        meta.insert("serum_asks".to_string(), asks.to_string());
        meta.insert("serum_event_queue".to_string(), eq.to_string());
    }
    if let Some(bv) = st.serum_base_vault {
        meta.insert("serum_base_vault".to_string(), bv.to_string());
    }
    if let Some(qv) = st.serum_quote_vault {
        meta.insert("serum_quote_vault".to_string(), qv.to_string());
    }
    if !meta.is_empty() {
        balance_update.metadata = Some(meta);
    }
    balance_update.set_dex_readiness_in_metadata(readiness);
    let subject = pool_subject(&pool_addr.to_string());
    match nats.jetstream_publish(&subject, &balance_update).await {
        Ok(true) => {
            info!(
                request_id = %request_id,
                pool = %pool_addr,
                readiness = ?readiness,
                "Raydium AMM serum backfill: published PoolCacheUpdate::BalanceUpdated to JetStream"
            );
            host.raydium_serum_fetched_insert(pool_addr);
            true
        }
        Ok(false) | Err(_) => {
            warn!(
                request_id = %request_id,
                pool = %pool_addr,
                "Raydium AMM serum backfill: JetStream publish failed after cache update"
            );
            host.raydium_serum_fetched_insert(pool_addr);
            true
        }
    }
}

/// Result of one cache-scoped Raydium AMM v4 RPC refresh (pool + vaults + optional Serum).
pub struct RaydiumAmmRpcRefreshRowResult {
    pub pool_addr: Pubkey,
    pub readiness: DexPoolReadiness,
    /// `true` when a non-`Observed` [`DexPoolReadiness`] update was published to JetStream.
    pub jetstream_published: bool,
}

/// Cold-path only: refresh one Raydium AMM v4 row already present in MASTER cache — no global scan.
/// Fetches pool account, vault balances, and Serum/OpenBook bids/asks/event_queue when `market_id`
/// is set and serum pointers are missing (same authoritative path as FIX-29 Geyser follow-up).
pub async fn cold_path_rpc_refresh_raydium_amm_pool_row(
    host: &impl ColdHost,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint: &Pubkey,
    pool_addr: Pubkey,
    mut state: RaydiumAmmState,
) -> Option<RaydiumAmmRpcRefreshRowResult> {
    let raydium_program = Pubkey::from_str(RAYDIUM_AMM_V4).expect("RAYDIUM_AMM_V4 constant");

    let pool_acc = match rpc.get_account_opt_retry(&pool_addr).await {
        Ok(Some(acc)) => acc,
        Ok(None) | Err(_) => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                "Raydium AMM RPC refresh: pool account fetch miss, skip"
            );
            return None;
        }
    };

    let parsed_state = match parse_pool_account(&raydium_program, &pool_acc.data) {
        Some(CachedPoolState::RaydiumAmm(s)) => s,
        _ => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                data_len = pool_acc.data.len(),
                "Raydium AMM RPC refresh: parse pool failed, skip"
            );
            return None;
        }
    };

    state.base_mint = parsed_state.base_mint;
    state.quote_mint = parsed_state.quote_mint;
    state.coin_vault = parsed_state.coin_vault;
    state.pc_vault = parsed_state.pc_vault;
    state.base_decimals = parsed_state.base_decimals;
    state.quote_decimals = parsed_state.quote_decimals;
    state.market_id = parsed_state.market_id;

    if state.base_mint != *base_mint && state.quote_mint != *base_mint {
        return None;
    }

    let coin_vault = state.coin_vault;
    let pc_vault = state.pc_vault;
    let bal_coin = match rpc.get_account_opt_retry(&coin_vault).await {
        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
        _ => None,
    };
    let bal_pc = match rpc.get_account_opt_retry(&pc_vault).await {
        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
        _ => None,
    };
    let (bc, bq) = match (bal_coin, bal_pc) {
        (Some(a), Some(b)) => (a, b),
        _ => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                "Raydium AMM RPC refresh: vault balance RPC incomplete, skip"
            );
            return None;
        }
    };

    state.coin_reserve = Some(bc);
    state.pc_reserve = Some(bq);

    host.live_pool_cache()
        .upsert(pool_addr, CachedPoolState::RaydiumAmm(state.clone()), 0);

    // Serum/OpenBook static accounts (same as FIX-29 one-shot path in Geyser follow-up).
    cold_path_backfill_raydium_serum_accounts(host, rpc, request_id, pool_addr, &state).await;

    let readiness = match host.live_pool_cache().get(&pool_addr).as_ref() {
        Some(CachedPoolState::RaydiumAmm(st)) => raydium_amm_readiness_for_pool_cache_update(st),
        _ => DexPoolReadiness::Observed,
    };
    host.live_pool_cache()
        .merge_raydium_amm_pool_readiness(pool_addr, readiness);

    if readiness == DexPoolReadiness::Observed {
        debug!(
            request_id = %request_id,
            pool = %pool_addr,
            mint = %base_mint,
            "Raydium AMM RPC refresh: still Observed after RPC, no JetStream publish"
        );
        return Some(RaydiumAmmRpcRefreshRowResult {
            pool_addr,
            readiness,
            jetstream_published: false,
        });
    }

    let jetstream_ok = if let Some(nats) = host.nats() {
        if let Some((CachedPoolState::RaydiumAmm(st), pool_slot, _age_ms)) =
            host.live_pool_cache().get_with_metadata(&pool_addr)
        {
            let mut balance_update = PoolCacheUpdate::new_balance_updated(
                "market-data",
                BUILD_VERSION,
                run_id,
                pool_addr.to_string(),
                "raydium".to_string(),
                st.base_mint.to_string(),
                st.quote_mint.to_string(),
                st.coin_reserve.unwrap_or(0),
                st.pc_reserve.unwrap_or(0),
                pool_slot,
            );
            let mut meta = std::collections::HashMap::new();
            if st.market_id != Pubkey::default() {
                meta.insert("market_id".to_string(), st.market_id.to_string());
            }
            if let (Some(bids), Some(asks), Some(eq)) =
                (st.serum_bids, st.serum_asks, st.serum_event_queue)
            {
                meta.insert("serum_bids".to_string(), bids.to_string());
                meta.insert("serum_asks".to_string(), asks.to_string());
                meta.insert("serum_event_queue".to_string(), eq.to_string());
            }
            if let Some(bv) = st.serum_base_vault {
                meta.insert("serum_base_vault".to_string(), bv.to_string());
            }
            if let Some(qv) = st.serum_quote_vault {
                meta.insert("serum_quote_vault".to_string(), qv.to_string());
            }
            if !meta.is_empty() {
                balance_update.metadata = Some(meta);
            }
            balance_update.set_dex_readiness_in_metadata(readiness);
            let subject = pool_subject(&pool_addr.to_string());
            match nats.jetstream_publish(&subject, &balance_update).await {
                Ok(true) => {
                    info!(
                        request_id = %request_id,
                        pool = %pool_addr,
                        mint = %base_mint,
                        readiness = ?readiness,
                        "Raydium AMM RPC refresh: published PoolCacheUpdate::BalanceUpdated to JetStream"
                    );
                    true
                }
                Ok(false) => {
                    warn!(
                        request_id = %request_id,
                        pool = %pool_addr,
                        "Raydium AMM RPC refresh: JetStream publish failed (timeout or drop)"
                    );
                    false
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        request_id = %request_id,
                        "Raydium AMM RPC refresh: Failed to publish PoolCacheUpdate to JetStream"
                    );
                    false
                }
            }
        } else {
            false
        }
    } else {
        false
    };

    if !jetstream_ok {
        warn!(
            request_id = %request_id,
            pool = %pool_addr,
            "Raydium AMM RPC refresh: MASTER cache updated but JetStream publish failed — SSOT drift risk"
        );
    }

    Some(RaydiumAmmRpcRefreshRowResult {
        pool_addr,
        readiness,
        jetstream_published: jetstream_ok,
    })
}

/// Result of one cache-scoped Raydium CPMM RPC refresh (pool + vault balances).
pub struct RaydiumCpmmRpcRefreshRowResult {
    pub pool_addr: Pubkey,
    pub readiness: DexPoolReadiness,
    /// `true` when a non-`Observed` [`DexPoolReadiness`] update was published to JetStream.
    pub jetstream_published: bool,
}

/// Cold-path only: refresh one Raydium CPMM row already present in MASTER cache — no global scan.
pub async fn cold_path_rpc_refresh_raydium_cpmm_pool_row(
    host: &impl ColdHost,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint: &Pubkey,
    pool_addr: Pubkey,
    mut state: RaydiumCpmmState,
) -> Option<RaydiumCpmmRpcRefreshRowResult> {
    let raydium_cpmm_program = Pubkey::from_str(RAYDIUM_CPMM).expect("RAYDIUM_CPMM constant");
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();

    let pool_acc = match rpc.get_account_opt_retry(&pool_addr).await {
        Ok(Some(acc)) => acc,
        Ok(None) | Err(_) => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                "Raydium CPMM RPC refresh: pool account fetch miss, skip"
            );
            return None;
        }
    };

    let parsed_state = match parse_pool_account(&raydium_cpmm_program, &pool_acc.data) {
        Some(CachedPoolState::RaydiumCpmm(s)) => s,
        _ => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                data_len = pool_acc.data.len(),
                "Raydium CPMM RPC refresh: parse pool failed, skip"
            );
            return None;
        }
    };

    state.token_0_mint = parsed_state.token_0_mint;
    state.token_1_mint = parsed_state.token_1_mint;
    state.token_0_vault = parsed_state.token_0_vault;
    state.token_1_vault = parsed_state.token_1_vault;

    if state.token_0_mint != *base_mint && state.token_1_mint != *base_mint {
        return None;
    }

    let vault0 = state.token_0_vault;
    let vault1 = state.token_1_vault;
    let bal0 = match rpc.get_account_opt_retry(&vault0).await {
        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
        _ => None,
    };
    let bal1 = match rpc.get_account_opt_retry(&vault1).await {
        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
        _ => None,
    };
    let (b0, b1) = match (bal0, bal1) {
        (Some(a), Some(b)) => (a, b),
        _ => {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                "Raydium CPMM RPC refresh: vault balance RPC incomplete, skip"
            );
            return None;
        }
    };

    state.reserve_0 = Some(b0);
    state.reserve_1 = Some(b1);

    host.live_pool_cache()
        .upsert(pool_addr, CachedPoolState::RaydiumCpmm(state.clone()), 0);

    let readiness = raydium_cpmm_readiness_for_pool_cache_update(&state);
    host.live_pool_cache()
        .merge_raydium_cpmm_pool_readiness(pool_addr, readiness);

    if readiness == DexPoolReadiness::Observed {
        debug!(
            request_id = %request_id,
            pool = %pool_addr,
            mint = %base_mint,
            "Raydium CPMM RPC refresh: still Observed after RPC, no JetStream publish"
        );
        return Some(RaydiumCpmmRpcRefreshRowResult {
            pool_addr,
            readiness,
            jetstream_published: false,
        });
    }

    let (pub_base_mint, pub_quote_mint, base_r, quote_r) = if state.token_1_mint == sol {
        (state.token_0_mint, state.token_1_mint, b0, b1)
    } else if state.token_0_mint == sol {
        (state.token_1_mint, state.token_0_mint, b1, b0)
    } else {
        (state.token_0_mint, state.token_1_mint, b0, b1)
    };

    let jetstream_ok = if let Some(nats) = host.nats() {
        let mut balance_update = PoolCacheUpdate::new_balance_updated(
            "market-data",
            BUILD_VERSION,
            run_id,
            pool_addr.to_string(),
            "raydium_cpmm".to_string(),
            pub_base_mint.to_string(),
            pub_quote_mint.to_string(),
            base_r,
            quote_r,
            0,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY.to_string(),
            raydium_cpmm_vaults_for_pool_cache_update(&state),
        );
        balance_update.metadata = Some(meta);
        balance_update.set_dex_readiness_in_metadata(readiness);
        let subject = pool_subject(&pool_addr.to_string());
        match nats.jetstream_publish(&subject, &balance_update).await {
            Ok(true) => {
                info!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    mint = %base_mint,
                    readiness = ?readiness,
                    "Raydium CPMM RPC refresh: published PoolCacheUpdate::BalanceUpdated to JetStream"
                );
                true
            }
            Ok(false) => {
                warn!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    "Raydium CPMM RPC refresh: JetStream publish failed (timeout or drop)"
                );
                false
            }
            Err(e) => {
                warn!(
                    error = %e,
                    request_id = %request_id,
                    "Raydium CPMM RPC refresh: Failed to publish PoolCacheUpdate to JetStream"
                );
                false
            }
        }
    } else {
        false
    };

    if !jetstream_ok {
        warn!(
            request_id = %request_id,
            pool = %pool_addr,
            "Raydium CPMM RPC refresh: MASTER cache updated but JetStream publish failed — SSOT drift risk"
        );
    }

    Some(RaydiumCpmmRpcRefreshRowResult {
        pool_addr,
        readiness,
        jetstream_published: jetstream_ok,
    })
}
