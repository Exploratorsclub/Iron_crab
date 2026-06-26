//! I-24d EnsureMeteora* cold-path handlers (DLMM + CPMM).

use super::host::{publish_control_response, ColdHost};
use super::rpc_refresh::{
    cold_path_rpc_refresh_meteora_cpmm_pool_row, cold_path_rpc_refresh_meteora_dlmm_pool_row,
};
use crate::execution::live_pool_cache::{CachedPoolState, MeteoraCpmmState, MeteoraState};
use crate::ipc::{ControlResponseStatus, DexPoolReadiness};
use crate::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// I-24d / Scope 21: Cold-path Meteora DLMM recovery — cache-scoped RPC refresh, JetStream SSOT, ControlResponse.
pub async fn handle_ensure_meteora_dlmm_pool_state(
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
            warn!(base_mint = %base_mint_str, error = %e, "EnsureMeteoraDlmmPoolState: invalid base_mint");
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
                .meteora_dlmm_pool_explicitly_ready(&pool_pk)
            {
                info!(
                    request_id = %request_id,
                    base_mint = %base_mint_str,
                    pool = %pool_pk,
                    "EnsureMeteoraDlmmPoolState: cache hit explicit Ready (skip RPC)"
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
            .base_mint_has_explicit_meteora_dlmm_ready_pool(&base_mint)
        {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                "EnsureMeteoraDlmmPoolState: mint already has explicit Ready Meteora DLMM pool (skip RPC)"
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
            "EnsureMeteoraDlmmPoolState: force_refresh — always running RPC refresh path (no cache-first short-circuit)"
        );
    }

    let mut candidate_pools: Vec<(Pubkey, MeteoraState)> = Vec::new();
    if let Some(pool_pk) = pool_hint_pk {
        match host.live_pool_cache().get(&pool_pk) {
            Some(CachedPoolState::Meteora(s)) => {
                if s.token_x_mint != base_mint && s.token_y_mint != base_mint {
                    warn!(
                        request_id = %request_id,
                        pool = %pool_pk,
                        base_mint = %base_mint_str,
                        "EnsureMeteoraDlmmPoolState: pool_address_hint DLMM row does not list base_mint"
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
                    "EnsureMeteoraDlmmPoolState: pool_address_hint is not a Meteora DLMM row"
                );
                if let Some(nats) = host.nats() {
                    publish_control_response(
                        nats,
                        host.run_id(),
                        request_id,
                        ControlResponseStatus::Error,
                        Some(pool_pk.to_string()),
                        Some("pool hint is not meteora dlmm in cache".to_string()),
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
                    "EnsureMeteoraDlmmPoolState: pool hint not in LivePoolCache (NotFound)"
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
            .meteora_dlmm_pools_for_mint(&base_mint);
        if candidate_pools.is_empty() {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                "EnsureMeteoraDlmmPoolState: no Meteora DLMM rows in LivePoolCache for mint (NotFound)"
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
        "EnsureMeteoraDlmmPoolState: starting cache-scoped Meteora DLMM RPC refresh"
    );

    let mut terminal_ok_pool: Option<Pubkey> = None;
    let mut ready_jetstream_failed_pool: Option<Pubkey> = None;

    for (pool_addr, state) in candidate_pools {
        if let Some(row) = cold_path_rpc_refresh_meteora_dlmm_pool_row(
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
                        "EnsureMeteoraDlmmPoolState: Partial published to JetStream — scan remaining candidates for Ready"
                    );
                }
                (DexPoolReadiness::Partial, false) => {
                    warn!(
                        request_id = %request_id,
                        pool = %row.pool_addr,
                        "EnsureMeteoraDlmmPoolState: Partial row MASTER-updated but JetStream publish failed"
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
                "EnsureMeteoraDlmmPoolState: terminal ok (Meteora DLMM Ready + JetStream publish)"
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
                "EnsureMeteoraDlmmPoolState: terminal error (Ready on MASTER but JetStream publish failed)"
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
        "EnsureMeteoraDlmmPoolState: RPC refresh did not yield Meteora DLMM Ready + JetStream publish (Error)"
    );
    if let Some(nats) = host.nats() {
        publish_control_response(
            nats,
            host.run_id(),
            request_id,
            ControlResponseStatus::Error,
            None,
            Some("meteora dlmm rpc refresh did not produce jetstream-ready state".to_string()),
        )
        .await;
    }
}

/// [`ControlResponse`].
pub async fn handle_ensure_meteora_cpmm_pool_state(
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
            warn!(base_mint = %base_mint_str, error = %e, "EnsureMeteoraCpmmPoolState: invalid base_mint");
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
                .meteora_cpmm_pool_explicitly_ready(&pool_pk)
            {
                info!(
                    request_id = %request_id,
                    base_mint = %base_mint_str,
                    pool = %pool_pk,
                    "EnsureMeteoraCpmmPoolState: cache hit explicit Ready (skip RPC)"
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
            .base_mint_has_explicit_meteora_cpmm_ready_pool(&base_mint)
        {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                "EnsureMeteoraCpmmPoolState: mint already has explicit Ready Meteora CPMM pool (skip RPC)"
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
            "EnsureMeteoraCpmmPoolState: force_refresh — always running RPC refresh path (no cache-first short-circuit)"
        );
    }

    let mut candidate_pools: Vec<(Pubkey, MeteoraCpmmState)> = Vec::new();
    if let Some(pool_pk) = pool_hint_pk {
        match host.live_pool_cache().get(&pool_pk) {
            Some(CachedPoolState::MeteoraCpmm(s)) => {
                if s.token_0_mint != base_mint && s.token_1_mint != base_mint {
                    warn!(
                        request_id = %request_id,
                        pool = %pool_pk,
                        base_mint = %base_mint_str,
                        "EnsureMeteoraCpmmPoolState: pool_address_hint Meteora CPMM row does not list base_mint"
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
                    "EnsureMeteoraCpmmPoolState: pool_address_hint is not a Meteora CPMM row"
                );
                if let Some(nats) = host.nats() {
                    publish_control_response(
                        nats,
                        host.run_id(),
                        request_id,
                        ControlResponseStatus::Error,
                        Some(pool_pk.to_string()),
                        Some("pool hint is not meteora cpmm in cache".to_string()),
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
                    "EnsureMeteoraCpmmPoolState: pool hint not in LivePoolCache (NotFound)"
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
            .meteora_cpmm_pools_for_mint(&base_mint);
        if candidate_pools.is_empty() {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                "EnsureMeteoraCpmmPoolState: no Meteora CPMM rows in LivePoolCache for mint (NotFound)"
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
        "EnsureMeteoraCpmmPoolState: starting cache-scoped Meteora CPMM RPC refresh"
    );

    let mut terminal_ok_pool: Option<Pubkey> = None;
    let mut ready_jetstream_failed_pool: Option<Pubkey> = None;

    for (pool_addr, state) in candidate_pools {
        if let Some(row) = cold_path_rpc_refresh_meteora_cpmm_pool_row(
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
                        "EnsureMeteoraCpmmPoolState: Partial published to JetStream — scan remaining candidates for Ready"
                    );
                }
                (DexPoolReadiness::Partial, false) => {
                    warn!(
                        request_id = %request_id,
                        pool = %row.pool_addr,
                        "EnsureMeteoraCpmmPoolState: Partial row MASTER-updated but JetStream publish failed"
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
                "EnsureMeteoraCpmmPoolState: terminal ok (Meteora CPMM Ready + JetStream publish)"
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
                "EnsureMeteoraCpmmPoolState: terminal error (Meteora CPMM Ready on MASTER but JetStream publish failed)"
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
        "EnsureMeteoraCpmmPoolState: RPC refresh did not yield Meteora CPMM Ready + JetStream publish (Error)"
    );
    if let Some(nats) = host.nats() {
        publish_control_response(
            nats,
            host.run_id(),
            request_id,
            ControlResponseStatus::Error,
            None,
            Some("meteora cpmm rpc refresh did not produce jetstream-ready state".to_string()),
        )
        .await;
    }
}
