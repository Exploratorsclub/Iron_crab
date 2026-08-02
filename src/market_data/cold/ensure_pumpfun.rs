//! I-24d EnsurePumpfunBondingCurve cold-path handler.

use super::host::{publish_control_response, ColdHost};
use super::publish_slot::resolve_cold_path_publish_slot;
use crate::execution::live_pool_cache::{CachedPoolState, PumpFunState};
use crate::ipc::{ControlResponseStatus, DexPoolReadiness, PoolCacheUpdate, NATIVE_SOL_MINT};
use crate::nats::jetstream::pool_subject;
use crate::solana::dex::pumpfun::{BondingCurveState, PumpFunDex};
use crate::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};

const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Cold-path recovery: fetch PumpFun bonding curve account via RPC (force_refresh), update MASTER
/// cache, publish JetStream PoolCacheUpdate. Does not short-circuit on cache when `force_refresh`.
pub async fn handle_ensure_pumpfun_bonding_curve(
    host: &impl ColdHost,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint_str: &str,
    pool_address_hint: Option<&str>,
    force_refresh: bool,
) {
    if super::defer::defer_discovery_if_md_state_pressure(host, request_id).await {
        return;
    }
    let base_mint = match Pubkey::from_str(base_mint_str) {
        Ok(p) => p,
        Err(e) => {
            warn!(base_mint = %base_mint_str, error = %e, "EnsurePumpfunBondingCurve: invalid base_mint");
            if let Some(nats) = host.nats() {
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    ControlResponseStatus::Error,
                    None,
                    Some(e.to_string()),
                )
                .await;
            }
            return;
        }
    };

    let pumpfun = match PumpFunDex::new(Arc::clone(rpc), Some(host.live_pool_cache_arc())) {
        Ok(d) => d,
        Err(e) => {
            warn!(error = %e, "EnsurePumpfunBondingCurve: PumpFunDex init failed");
            if let Some(nats) = host.nats() {
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    ControlResponseStatus::Error,
                    None,
                    Some(e.to_string()),
                )
                .await;
            }
            return;
        }
    };

    let (bonding_curve, _) = pumpfun.derive_bonding_curve(&base_mint);
    let bonding_curve = pool_address_hint
        .and_then(|s| Pubkey::from_str(s).ok())
        .unwrap_or(bonding_curve);
    let bonding_curve_str = bonding_curve.to_string();
    let had_master_pool = host.live_pool_cache().get(&bonding_curve).is_some();

    if !force_refresh {
        if let Some(CachedPoolState::PumpFun(_)) = host.live_pool_cache().get(&bonding_curve) {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                pool = %bonding_curve_str,
                "EnsurePumpfunBondingCurve: cache hit (skip RPC)"
            );
            if let Some(nats) = host.nats() {
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    ControlResponseStatus::Ok,
                    Some(bonding_curve_str),
                    None,
                )
                .await;
            }
            return;
        }
    } else {
        warn!(
            request_id = %request_id,
            base_mint = %base_mint_str,
            pool = %bonding_curve_str,
            "EnsurePumpfunBondingCurve: force_refresh — always fetching bonding curve via RPC (no cache-first)"
        );
    }

    let (acct, rpc_context_slot) = match rpc.get_account_with_slot_retry(&bonding_curve).await {
        Ok(pair) => pair,
        Err(e) => {
            warn!(
                request_id = %request_id,
                pool = %bonding_curve_str,
                error = %e,
                "EnsurePumpfunBondingCurve: RPC get_account failed"
            );
            if let Some(nats) = host.nats() {
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    ControlResponseStatus::Error,
                    Some(bonding_curve_str),
                    Some(e.to_string()),
                )
                .await;
            }
            return;
        }
    };
    let state = match BondingCurveState::parse(&acct.data) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                request_id = %request_id,
                pool = %bonding_curve_str,
                error = %e,
                "EnsurePumpfunBondingCurve: parse bonding curve failed"
            );
            if let Some(nats) = host.nats() {
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    ControlResponseStatus::Error,
                    Some(bonding_curve_str),
                    Some(format!("parse error: {e}")),
                )
                .await;
            }
            return;
        }
    };

    let mut publish_slot = resolve_cold_path_publish_slot(rpc_context_slot);
    if publish_slot == 0 {
        if let Ok(slot) = rpc.get_slot_retry().await {
            publish_slot = slot;
        }
    }
    if publish_slot == 0 {
        warn!(
            request_id = %request_id,
            pool = %bonding_curve_str,
            rpc_context_slot,
            "EnsurePumpfunBondingCurve: no publish slot watermark (RPC context, Geyser head, getSlot all zero)"
        );
    }

    let token_program = host
        .live_pool_cache()
        .get_mint_program(&base_mint)
        .unwrap_or_else(|| Pubkey::new_from_array(spl_token::id().to_bytes()));
    let (associated_bonding_curve, _) =
        pumpfun.derive_associated_bonding_curve(&bonding_curve, &base_mint, &token_program);

    let cached = CachedPoolState::PumpFun(PumpFunState {
        token_mint: base_mint,
        bonding_curve,
        associated_bonding_curve,
        virtual_sol_reserves: state.virtual_sol_reserves,
        virtual_token_reserves: state.virtual_token_reserves,
        real_sol_reserves: state.real_sol_reserves,
        real_token_reserves: state.real_token_reserves,
        complete: state.complete,
        creator: state.creator,
        cashback_enabled: state.cashback_enabled,
    });
    host.live_pool_cache()
        .upsert(bonding_curve, cached, publish_slot);

    let mut meta = std::collections::HashMap::new();
    meta.insert("creator".to_string(), state.creator.to_string());
    meta.insert(
        "associated_bonding_curve".to_string(),
        associated_bonding_curve.to_string(),
    );
    meta.insert("complete".to_string(), state.complete.to_string());
    meta.insert(
        "real_token_reserves".to_string(),
        state.real_token_reserves.to_string(),
    );
    meta.insert(
        "real_sol_reserves".to_string(),
        state.real_sol_reserves.to_string(),
    );
    meta.insert(
        "cashback_enabled".to_string(),
        state.cashback_enabled.to_string(),
    );

    let use_balance_updated = had_master_pool || force_refresh;
    let mut pool_update = if use_balance_updated {
        let mut bal = PoolCacheUpdate::new_balance_updated(
            "market-data",
            BUILD_VERSION,
            run_id,
            bonding_curve_str.clone(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            NATIVE_SOL_MINT.to_string(),
            state.virtual_token_reserves,
            state.virtual_sol_reserves,
            publish_slot,
        );
        bal.metadata = Some(meta.clone());
        bal
    } else {
        let mut disc = PoolCacheUpdate::new_pool_discovered(
            "market-data",
            BUILD_VERSION,
            run_id,
            bonding_curve_str.clone(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            NATIVE_SOL_MINT.to_string(),
            state.virtual_token_reserves,
            state.virtual_sol_reserves,
            Some(publish_slot),
            publish_slot,
        );
        disc.metadata = Some(meta);
        disc
    };
    // Cold-path RPC refresh: explicit Ready for JetStream / SLAVE (Bug #36).
    pool_update.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
    host.live_pool_cache()
        .merge_pumpfun_bonding_readiness(bonding_curve, DexPoolReadiness::Ready);

    let jetstream_ok = if let Some(nats) = host.nats() {
        let subject = pool_subject(&bonding_curve_str);
        match nats.jetstream_publish(&subject, &pool_update).await {
            Ok(true) => {
                info!(
                    pool = %bonding_curve_str,
                    base_mint = %base_mint_str,
                    publish_slot,
                    rpc_context_slot,
                    update_type = if use_balance_updated { "BalanceUpdated" } else { "PoolDiscovered" },
                    "EnsurePumpfunBondingCurve: Published PoolCacheUpdate to JetStream"
                );
                true
            }
            Ok(false) => {
                warn!("EnsurePumpfunBondingCurve: JetStream publish failed (timeout or drop)");
                false
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "EnsurePumpfunBondingCurve: Failed to publish PoolCacheUpdate to JetStream"
                );
                false
            }
        }
    } else {
        false
    };

    if let Some(nats) = host.nats() {
        let (status, message) = if jetstream_ok {
            (ControlResponseStatus::Ok, None)
        } else {
            (
                ControlResponseStatus::Error,
                Some("JetStream publish failed".to_string()),
            )
        };
        publish_control_response(
            nats,
            host.run_id(),
            request_id,
            status,
            Some(bonding_curve_str),
            message,
        )
        .await;
    }
}
