//! I-24d EnsureRaydium* cold-path handlers (AMM + CPMM).

use super::host::{publish_control_response, ColdHost};
use super::rpc_refresh::{
    cold_path_rpc_refresh_raydium_amm_pool_row, cold_path_rpc_refresh_raydium_cpmm_pool_row,
};
use crate::execution::live_pool_cache::{CachedPoolState, RaydiumAmmState, RaydiumCpmmState};
use crate::ipc::{ControlResponseStatus, DexPoolReadiness};
use crate::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// I-24d Scope 22: Cold-path Raydium AMM v4 recovery — cache-scoped RPC refresh, JetStream SSOT,
/// [`ControlResponse`].
pub async fn handle_ensure_raydium_amm_pool_state(
    host: &impl ColdHost,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint_str: &str,
    pool_address_hint: Option<&str>,
    force_refresh: bool,
) {
    let base_mint = match Pubkey::from_str(base_mint_str) {
        Ok(p) => p,
        Err(e) => {
            warn!(base_mint = %base_mint_str, error = %e, "EnsureRaydiumAmmPoolState: invalid base_mint");
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

    let pool_hint_pk = pool_address_hint.and_then(|s| Pubkey::from_str(s).ok());

    if !force_refresh {
        if let Some(pool_pk) = pool_hint_pk {
            if host
                .live_pool_cache()
                .raydium_amm_pool_explicitly_ready(&pool_pk)
            {
                info!(
                    request_id = %request_id,
                    base_mint = %base_mint_str,
                    pool = %pool_pk,
                    "EnsureRaydiumAmmPoolState: cache hit explicit Ready (skip RPC)"
                );
                if let Some(nats) = host.nats() {
                    publish_control_response(
                        nats,
                        host.run_id(),
                        request_id,
                        ControlResponseStatus::Ok,
                        Some(pool_pk.to_string()),
                        None,
                    )
                    .await;
                }
                return;
            }
        } else if host
            .live_pool_cache()
            .base_mint_has_explicit_raydium_amm_ready_pool(&base_mint)
        {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                "EnsureRaydiumAmmPoolState: mint already has explicit Ready Raydium AMM pool (skip RPC)"
            );
            if let Some(nats) = host.nats() {
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    ControlResponseStatus::Ok,
                    None,
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
            pool_address_hint = ?pool_address_hint,
            "EnsureRaydiumAmmPoolState: force_refresh — always running RPC refresh path (no cache-first short-circuit)"
        );
    }

    let mut candidate_pools: Vec<(Pubkey, RaydiumAmmState)> = Vec::new();
    if let Some(pool_pk) = pool_hint_pk {
        match host.live_pool_cache().get(&pool_pk) {
            Some(CachedPoolState::RaydiumAmm(s)) => {
                if s.base_mint != base_mint && s.quote_mint != base_mint {
                    warn!(
                        request_id = %request_id,
                        pool = %pool_pk,
                        base_mint = %base_mint_str,
                        "EnsureRaydiumAmmPoolState: pool_address_hint Raydium AMM row does not list base_mint"
                    );
                    if let Some(nats) = host.nats() {
                        publish_control_response(
                            nats,
                            host.run_id(),
                            request_id,
                            ControlResponseStatus::Error,
                            Some(pool_pk.to_string()),
                            Some("pool hint mint mismatch".to_string()),
                        )
                        .await;
                    }
                    return;
                }
                candidate_pools.push((pool_pk, s));
            }
            Some(_) => {
                warn!(
                    request_id = %request_id,
                    pool = %pool_pk,
                    "EnsureRaydiumAmmPoolState: pool_address_hint is not a Raydium AMM row"
                );
                if let Some(nats) = host.nats() {
                    publish_control_response(
                        nats,
                        host.run_id(),
                        request_id,
                        ControlResponseStatus::Error,
                        Some(pool_pk.to_string()),
                        Some("pool hint is not raydium amm in cache".to_string()),
                    )
                    .await;
                }
                return;
            }
            None => {
                info!(
                    request_id = %request_id,
                    pool = %pool_pk,
                    base_mint = %base_mint_str,
                    "EnsureRaydiumAmmPoolState: pool hint not in LivePoolCache (NotFound)"
                );
                if let Some(nats) = host.nats() {
                    publish_control_response(
                        nats,
                        host.run_id(),
                        request_id,
                        ControlResponseStatus::NotFound,
                        Some(pool_pk.to_string()),
                        None,
                    )
                    .await;
                }
                return;
            }
        }
    } else {
        candidate_pools = host
            .live_pool_cache()
            .raydium_amm_pools_for_mint(&base_mint);
        if candidate_pools.is_empty() {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                "EnsureRaydiumAmmPoolState: no Raydium AMM rows in LivePoolCache for mint (NotFound)"
            );
            if let Some(nats) = host.nats() {
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    ControlResponseStatus::NotFound,
                    None,
                    None,
                )
                .await;
            }
            return;
        }
    }

    info!(
        request_id = %request_id,
        base_mint = %base_mint_str,
        pool_address_hint = ?pool_address_hint,
        candidates = candidate_pools.len(),
        force_refresh = %force_refresh,
        "EnsureRaydiumAmmPoolState: starting cache-scoped Raydium AMM RPC refresh"
    );

    let mut terminal_ok_pool: Option<Pubkey> = None;
    let mut ready_jetstream_failed_pool: Option<Pubkey> = None;

    for (pool_addr, state) in candidate_pools {
        if let Some(row) = cold_path_rpc_refresh_raydium_amm_pool_row(
            host, rpc, run_id, request_id, &base_mint, pool_addr, state,
        )
        .await
        {
            match (row.readiness, row.jetstream_published) {
                (DexPoolReadiness::Ready, true) => {
                    terminal_ok_pool = Some(row.pool_addr);
                    break;
                }
                (DexPoolReadiness::Ready, false) => {
                    ready_jetstream_failed_pool = Some(row.pool_addr);
                }
                (DexPoolReadiness::Partial, true) => {
                    debug!(
                        request_id = %request_id,
                        pool = %row.pool_addr,
                        "EnsureRaydiumAmmPoolState: Partial published to JetStream — scan remaining candidates for Ready"
                    );
                }
                (DexPoolReadiness::Partial, false) => {
                    warn!(
                        request_id = %request_id,
                        pool = %row.pool_addr,
                        "EnsureRaydiumAmmPoolState: Partial row MASTER-updated but JetStream publish failed"
                    );
                }
                (DexPoolReadiness::Observed, _) => {}
            }
        }
    }

    if let Some(pool_pk) = terminal_ok_pool {
        if let Some(nats) = host.nats() {
            let pool_str = pool_pk.to_string();
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                pool = %pool_str,
                readiness = ?DexPoolReadiness::Ready,
                jetstream = "published",
                "EnsureRaydiumAmmPoolState: terminal ok (Raydium AMM Ready + JetStream publish)"
            );
            publish_control_response(
                nats,
                host.run_id(),
                request_id,
                ControlResponseStatus::Ok,
                Some(pool_str),
                None,
            )
            .await;
        }
        return;
    }

    if let Some(pool_pk) = ready_jetstream_failed_pool {
        if let Some(nats) = host.nats() {
            let pool_str = pool_pk.to_string();
            let msg = "JetStream publish failed".to_string();
            warn!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                pool = %pool_str,
                error = %msg,
                "EnsureRaydiumAmmPoolState: terminal error (Raydium AMM Ready on MASTER but JetStream publish failed)"
            );
            publish_control_response(
                nats,
                host.run_id(),
                request_id,
                ControlResponseStatus::Error,
                Some(pool_str),
                Some(msg),
            )
            .await;
        }
        return;
    }

    warn!(
        request_id = %request_id,
        base_mint = %base_mint_str,
        "EnsureRaydiumAmmPoolState: RPC refresh did not yield Raydium AMM Ready + JetStream publish (Error)"
    );
    if let Some(nats) = host.nats() {
        publish_control_response(
            nats,
            host.run_id(),
            request_id,
            ControlResponseStatus::Error,
            None,
            Some("raydium amm rpc refresh did not produce jetstream-ready state".to_string()),
        )
        .await;
    }
}

/// [`ControlResponse`].
pub async fn handle_ensure_raydium_cpmm_pool_state(
    host: &impl ColdHost,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint_str: &str,
    pool_address_hint: Option<&str>,
    force_refresh: bool,
) {
    let base_mint = match Pubkey::from_str(base_mint_str) {
        Ok(p) => p,
        Err(e) => {
            warn!(base_mint = %base_mint_str, error = %e, "EnsureRaydiumCpmmPoolState: invalid base_mint");
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

    let pool_hint_pk = pool_address_hint.and_then(|s| Pubkey::from_str(s).ok());

    if !force_refresh {
        if let Some(pool_pk) = pool_hint_pk {
            if host
                .live_pool_cache()
                .raydium_cpmm_pool_explicitly_ready(&pool_pk)
            {
                info!(
                    request_id = %request_id,
                    base_mint = %base_mint_str,
                    pool = %pool_pk,
                    "EnsureRaydiumCpmmPoolState: cache hit explicit Ready (skip RPC)"
                );
                if let Some(nats) = host.nats() {
                    publish_control_response(
                        nats,
                        host.run_id(),
                        request_id,
                        ControlResponseStatus::Ok,
                        Some(pool_pk.to_string()),
                        None,
                    )
                    .await;
                }
                return;
            }
        } else if host
            .live_pool_cache()
            .base_mint_has_explicit_raydium_cpmm_ready_pool(&base_mint)
        {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                "EnsureRaydiumCpmmPoolState: mint already has explicit Ready Raydium CPMM pool (skip RPC)"
            );
            if let Some(nats) = host.nats() {
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    ControlResponseStatus::Ok,
                    None,
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
            pool_address_hint = ?pool_address_hint,
            "EnsureRaydiumCpmmPoolState: force_refresh — always running RPC refresh path (no cache-first short-circuit)"
        );
    }

    let mut candidate_pools: Vec<(Pubkey, RaydiumCpmmState)> = Vec::new();
    if let Some(pool_pk) = pool_hint_pk {
        match host.live_pool_cache().get(&pool_pk) {
            Some(CachedPoolState::RaydiumCpmm(s)) => {
                if s.token_0_mint != base_mint && s.token_1_mint != base_mint {
                    warn!(
                        request_id = %request_id,
                        pool = %pool_pk,
                        base_mint = %base_mint_str,
                        "EnsureRaydiumCpmmPoolState: pool_address_hint Raydium CPMM row does not list base_mint"
                    );
                    if let Some(nats) = host.nats() {
                        publish_control_response(
                            nats,
                            host.run_id(),
                            request_id,
                            ControlResponseStatus::Error,
                            Some(pool_pk.to_string()),
                            Some("pool hint mint mismatch".to_string()),
                        )
                        .await;
                    }
                    return;
                }
                candidate_pools.push((pool_pk, s));
            }
            Some(_) => {
                warn!(
                    request_id = %request_id,
                    pool = %pool_pk,
                    "EnsureRaydiumCpmmPoolState: pool_address_hint is not a Raydium CPMM row"
                );
                if let Some(nats) = host.nats() {
                    publish_control_response(
                        nats,
                        host.run_id(),
                        request_id,
                        ControlResponseStatus::Error,
                        Some(pool_pk.to_string()),
                        Some("pool hint is not raydium cpmm in cache".to_string()),
                    )
                    .await;
                }
                return;
            }
            None => {
                info!(
                    request_id = %request_id,
                    pool = %pool_pk,
                    base_mint = %base_mint_str,
                    "EnsureRaydiumCpmmPoolState: pool hint not in LivePoolCache (NotFound)"
                );
                if let Some(nats) = host.nats() {
                    publish_control_response(
                        nats,
                        host.run_id(),
                        request_id,
                        ControlResponseStatus::NotFound,
                        Some(pool_pk.to_string()),
                        None,
                    )
                    .await;
                }
                return;
            }
        }
    } else {
        candidate_pools = host
            .live_pool_cache()
            .raydium_cpmm_pools_for_mint(&base_mint);
        if candidate_pools.is_empty() {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                "EnsureRaydiumCpmmPoolState: no Raydium CPMM rows in LivePoolCache for mint (NotFound)"
            );
            if let Some(nats) = host.nats() {
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    ControlResponseStatus::NotFound,
                    None,
                    None,
                )
                .await;
            }
            return;
        }
    }

    info!(
        request_id = %request_id,
        base_mint = %base_mint_str,
        pool_address_hint = ?pool_address_hint,
        candidates = candidate_pools.len(),
        force_refresh = %force_refresh,
        "EnsureRaydiumCpmmPoolState: starting cache-scoped Raydium CPMM RPC refresh"
    );

    let mut terminal_ok_pool: Option<Pubkey> = None;
    let mut ready_jetstream_failed_pool: Option<Pubkey> = None;

    for (pool_addr, state) in candidate_pools {
        if let Some(row) = cold_path_rpc_refresh_raydium_cpmm_pool_row(
            host, rpc, run_id, request_id, &base_mint, pool_addr, state,
        )
        .await
        {
            match (row.readiness, row.jetstream_published) {
                (DexPoolReadiness::Ready, true) => {
                    terminal_ok_pool = Some(row.pool_addr);
                    break;
                }
                (DexPoolReadiness::Ready, false) => {
                    ready_jetstream_failed_pool = Some(row.pool_addr);
                }
                (DexPoolReadiness::Partial, true) => {
                    debug!(
                        request_id = %request_id,
                        pool = %row.pool_addr,
                        "EnsureRaydiumCpmmPoolState: Partial published to JetStream — scan remaining candidates for Ready"
                    );
                }
                (DexPoolReadiness::Partial, false) => {
                    warn!(
                        request_id = %request_id,
                        pool = %row.pool_addr,
                        "EnsureRaydiumCpmmPoolState: Partial row MASTER-updated but JetStream publish failed"
                    );
                }
                (DexPoolReadiness::Observed, _) => {}
            }
        }
    }

    if let Some(pool_pk) = terminal_ok_pool {
        if let Some(nats) = host.nats() {
            let pool_str = pool_pk.to_string();
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                pool = %pool_str,
                readiness = ?DexPoolReadiness::Ready,
                jetstream = "published",
                "EnsureRaydiumCpmmPoolState: terminal ok (Raydium CPMM Ready + JetStream publish)"
            );
            publish_control_response(
                nats,
                host.run_id(),
                request_id,
                ControlResponseStatus::Ok,
                Some(pool_str),
                None,
            )
            .await;
        }
        return;
    }

    if let Some(pool_pk) = ready_jetstream_failed_pool {
        if let Some(nats) = host.nats() {
            let pool_str = pool_pk.to_string();
            let msg = "JetStream publish failed".to_string();
            warn!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                pool = %pool_str,
                error = %msg,
                "EnsureRaydiumCpmmPoolState: terminal error (Raydium CPMM Ready on MASTER but JetStream publish failed)"
            );
            publish_control_response(
                nats,
                host.run_id(),
                request_id,
                ControlResponseStatus::Error,
                Some(pool_str),
                Some(msg),
            )
            .await;
        }
        return;
    }

    warn!(
        request_id = %request_id,
        base_mint = %base_mint_str,
        "EnsureRaydiumCpmmPoolState: RPC refresh did not yield Raydium CPMM Ready + JetStream publish (Error)"
    );
    if let Some(nats) = host.nats() {
        publish_control_response(
            nats,
            host.run_id(),
            request_id,
            ControlResponseStatus::Error,
            None,
            Some("raydium cpmm rpc refresh did not produce jetstream-ready state".to_string()),
        )
        .await;
    }
}
