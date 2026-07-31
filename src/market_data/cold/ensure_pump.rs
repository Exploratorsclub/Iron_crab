//! I-24d EnsurePumpAmmPoolAccounts cold-path handler.

use super::host::{publish_control_response, ColdHost};
use super::publish_slot::resolve_cold_path_publish_slot;
use crate::execution::live_pool_cache::{CachedPoolState, PumpAmmState};
use crate::ipc::{ControlResponseStatus, PoolCacheUpdate};
use crate::nats::jetstream::pool_subject;
use crate::solana::dex::pumpfun_amm::pump_amm_inferred_sell_ix_account_count;
use crate::solana::dex::pumpfun_amm::PumpAmmPoolAccountsDiagnostic;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tracing::{info, warn};

const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Cold Path: merge existing PumpAmm reserves with authoritative vault RPC reads for I-24d Ensure.
///
/// - If the cache already has **both** reserves present and strictly positive, returns `Ok` with
///   those values (skip vault RPC).
/// - Otherwise vault RPC **must** succeed. On RPC failure there is **no** silent fallback to
///   `0/0` — that would let the handler publish a false “success” SSOT (I-24d / I-12).
async fn resolve_pump_amm_reserves_for_ensure_discovery(
    request_id: &str,
    base_mint_str: &str,
    pool_address_str: &str,
    existing: Option<&CachedPoolState>,
    pool_accounts: &[Pubkey],
    dex: &crate::solana::dex::pumpfun_amm::PumpFunAmmDex,
    force_refresh: bool,
) -> Result<(u64, u64), String> {
    if !force_refresh {
        if let Some(CachedPoolState::PumpAmm(s)) = existing {
            if let (Some(br), Some(qr)) = (s.base_reserve, s.quote_reserve) {
                if br > 0 && qr > 0 {
                    info!(
                        request_id = %request_id,
                        base_mint = %base_mint_str,
                        pool = %pool_address_str,
                        base_reserve = br,
                        quote_reserve = qr,
                        "EnsurePumpAmmPoolAccounts: using existing non-degenerate reserves (skip vault RPC)"
                    );
                    return Ok((br, qr));
                }
            }
        }
    } else {
        warn!(
            request_id = %request_id,
            base_mint = %base_mint_str,
            pool = %pool_address_str,
            "EnsurePumpAmmPoolAccounts: force_refresh — always hydrating vault reserves via RPC (no cache-first reserves)"
        );
    }

    match dex.fetch_pump_amm_vault_reserves(pool_accounts).await {
        Ok((b, q)) => {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                pool = %pool_address_str,
                base_reserve = b,
                quote_reserve = q,
                "EnsurePumpAmmPoolAccounts: hydrated vault reserves via RPC (Cold Path)"
            );
            Ok((b, q))
        }
        Err(e) => {
            warn!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                pool = %pool_address_str,
                error = %e,
                "EnsurePumpAmmPoolAccounts: vault reserve RPC failed (no non-degenerate cached reserves)"
            );
            Err(format!("vault reserve RPC failed: {e}"))
        }
    }
}

/// Scope 44: one structured `info!` for cold-path PumpSwap v14 provenance (SIM 6023 forensics).
fn log_pump_amm_scope44_pool_accounts_diag(
    request_id: &str,
    base_mint_str: &str,
    force_refresh: bool,
    diag: &PumpAmmPoolAccountsDiagnostic,
    v14: &[Pubkey],
) {
    let ref_sig = diag.reference_swap_signature.as_deref().unwrap_or("");
    info!(
        request_id = %request_id,
        base_mint = %base_mint_str,
        scope = "44",
        pump_amm_diag_source = diag.source,
        pool_market = %diag.pool_market,
        diag_force_refresh = diag.force_refresh,
        request_force_refresh = %force_refresh,
        ref_swap_sig = %ref_sig,
        v14_csv = %PumpAmmPoolAccountsDiagnostic::format_v14_csv(v14),
        pfr = %v14.get(6).copied().unwrap_or_default(),
        pfr_field = ?diag.protocol_fee_recipient.resolution,
        pfr_tag = diag.protocol_fee_recipient.tag,
        pfr_ta = %v14.get(7).copied().unwrap_or_default(),
        pfr_ta_field = ?diag.protocol_fee_recipient_ta.resolution,
        pfr_ta_tag = diag.protocol_fee_recipient_ta.tag,
        cc_vault_ata = %v14.get(9).copied().unwrap_or_default(),
        cc_vault_ata_field = ?diag.coin_creator_vault_ata.resolution,
        cc_vault_ata_tag = diag.coin_creator_vault_ata.tag,
        cc_auth = %v14.get(10).copied().unwrap_or_default(),
        cc_auth_field = ?diag.coin_creator_vault_authority.resolution,
        cc_auth_tag = diag.coin_creator_vault_authority.tag,
        gva = %v14.get(11).copied().unwrap_or_default(),
        gva_field = ?diag.global_volume_accumulator.resolution,
        gva_tag = diag.global_volume_accumulator.tag,
        fee_cfg_cached = %v14.get(12).copied().unwrap_or_default(),
        fee_cfg_field = ?diag.fee_config.resolution,
        fee_cfg_tag = diag.fee_config.tag,
        fee_prog_cached = %v14.get(13).copied().unwrap_or_default(),
        fee_prog_field = ?diag.fee_program.resolution,
        fee_prog_tag = diag.fee_program.tag,
        "I-24d Scope44: pump_amm v14 diagnostic — ref_sig=successful swap TX for manual diff; SELL ix uses global fee_config + fee_program (5PH… / pfee…); v14[12]/[13] may differ (cache row)"
    );
}

/// Scope 49: single structured line before ControlResponse=Error when force_refresh SELL layout stays unresolved.
fn log_pump_amm_scope49_force_refresh_sell_layout_failure(
    request_id: &str,
    pool_address_str: &str,
    base_mint_str: &str,
    diag: Option<&crate::solana::dex::pumpfun_amm::PumpAmmForceRefreshSellLayoutDiag>,
) {
    let Some(d) = diag else {
        warn!(
            request_id = %request_id,
            pool = %pool_address_str,
            base_mint = %base_mint_str,
            sell_layout_failure_class = "force_refresh_diag_missing",
            "I-24d Scope49: sell_layout_ready=false after force_refresh (no structured diag from dex — check pumpfun_amm version)"
        );
        return;
    };
    let ext = d.last_external.as_ref();
    warn!(
        request_id = %request_id,
        pool = %pool_address_str,
        base_mint = %base_mint_str,
        scope = "49",
        local_history_empty = d.local_history_empty,
        local_observation_failed = d.local_observation_failed,
        local_history_probe = %d.local_history_probe.as_log_str(),
        external_attempted = d.external_attempted,
        external_sig_limit = ?d.external_sig_limit,
        external_max_tx_fetches = ?d.external_max_tx_fetches,
        external_max_get_transaction_calls = ?d.external_max_get_transaction_calls,
        termination_reason = %d.termination_reason.as_log_str(),
        ext_elapsed_ms = ext.map(|s| s.elapsed_total_ms),
        ext_get_signatures_calls = ext.map(|s| s.get_signatures_calls),
        ext_get_transaction_calls = ext.map(|s| s.get_transaction_calls),
        ext_signatures_returned = ext.map(|s| s.signatures_returned_last),
        ext_transactions_fetched = ext.map(|s| s.transactions_fetched),
        ext_pump_amm_ix_seen = ext.map(|s| s.pump_amm_instructions_seen),
        ext_pump_amm_sell_discriminator_seen = ext.map(|s| s.pump_amm_sell_discriminator_seen),
        ext_sell_candidates_seen = ext.map(|s| s.sell_candidates_seen),
        ext_provider_status = ext.map(|s| s.provider_status_last.as_log_str()),
        ext_termination = ext.map(|s| s.termination_reason.as_log_str()),
        "I-24d Scope49: sell_layout_ready=false after force_refresh — classification for supervisor (timeout vs 429 vs budget vs empty)"
    );
}

/// I-24d: Handle EnsurePumpAmmPoolAccounts Discovery Request.
///
/// Performs RPC-based discovery (Cold Path), updates MASTER cache, publishes
/// JetStream PoolCacheUpdate (SSOT), and ControlResponse (correlation only).
/// Uses shared PumpFunAmmDex for dedupe/storm protection.
/// When pool_address_hint is provided, uses fast getAccount path (<1s) instead of slow getProgramAccounts.
pub async fn handle_ensure_pump_amm_pool_accounts(
    host: &impl ColdHost,
    dex: &crate::solana::dex::pumpfun_amm::PumpFunAmmDex,
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
            warn!(base_mint = %base_mint_str, error = %e, "EnsurePumpAmmPoolAccounts: invalid base_mint");
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

    let pool_hint = pool_address_hint.and_then(|s| Pubkey::from_str(s).ok());
    info!(
        request_id = %request_id,
        base_mint = %base_mint_str,
        pool_address_hint = ?pool_address_hint,
        has_pool_hint = pool_hint.is_some(),
        force_refresh = %force_refresh,
        "I-24d Discovery: EnsurePumpAmmPoolAccounts start (Geyser cache check then RPC fast path if hint)"
    );
    match dex
        .pool_accounts_v1_for_base_mint_with_hint_diagnostic(base_mint, pool_hint, force_refresh)
        .await
    {
        Ok(Some(wrapped)) if wrapped.accounts.len() >= 14 => {
            let sell_cashback_remaining = wrapped.sell_requires_cashback_remaining;
            let sell_cashback_third = wrapped.sell_cashback_third_meta;
            let sell_tail_0 = wrapped.sell_extended_tail_0;
            let sell_tail_1 = wrapped.sell_extended_tail_1;
            let sell_fee_tail_0 = wrapped.sell_extended_fee_tail_0;
            let sell_fee_tail_1 = wrapped.sell_extended_fee_tail_1;
            let sell_requires_pre_fee_metas = wrapped.sell_requires_pre_fee_metas;
            let sell_pre_fee_meta_1 = wrapped.sell_pre_fee_meta_1;
            let sell_layout_ready_from_refresh = wrapped.sell_layout_ready;
            let pump_amm_force_refresh_sell_diag = wrapped.force_refresh_sell_layout_diag;
            let accounts = wrapped.accounts;
            let diag = wrapped.diagnostic;
            log_pump_amm_scope44_pool_accounts_diag(
                request_id,
                base_mint_str,
                force_refresh,
                &diag,
                &accounts,
            );
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                outcome = "ok",
                rpc_global_scan = "no",
                "I-24d Discovery: pump_amm pool_accounts resolved (terminal)"
            );
            let pool_address = accounts[0];
            let pool_address_str = pool_address.to_string();
            let base_mint_str = base_mint.to_string();
            let quote_mint = accounts[3];
            let quote_mint_str = quote_mint.to_string();

            let existing = host.live_pool_cache().get(&pool_address);
            let creator_opt = existing.as_ref().and_then(|st| match st {
                CachedPoolState::PumpAmm(s) => s.creator,
                _ => None,
            });
            let (ext_flag_cache, ext_third_cache, ext_t0_cache, ext_t1_cache) = host
                .live_pool_cache()
                .pump_amm_sell_extended_layout(&pool_address);
            let (ext_fee_t0_cache, ext_fee_t1_cache) = host
                .live_pool_cache()
                .pump_amm_sell_fee_tail_layout(&pool_address);
            let ext_requires_fee_cache = host
                .live_pool_cache()
                .pump_amm_sell_requires_fee_tail(&pool_address);
            let ext_requires_pre_fee_cache = host
                .live_pool_cache()
                .pump_amm_sell_requires_pre_fee_metas(&pool_address);
            let ext_pre_fee_meta_1_cache = host
                .live_pool_cache()
                .pump_amm_sell_pre_fee_meta_1(&pool_address);
            let refresh_requires_fee_tail = sell_fee_tail_0.is_some() && sell_fee_tail_1.is_some();
            let (
                sell_flag_merged,
                third_merged,
                tail0_merged,
                tail1_merged,
                fee_tail0_merged,
                fee_tail1_merged,
                requires_fee_tail_merged,
                requires_pre_fee_metas_merged,
                pre_fee_meta_1_merged,
                sell_layout_ready,
                dex_readiness,
            ) = super::pump_layout::pump_amm_sell_layout_state_for_ensure_publish(
                force_refresh,
                ext_flag_cache,
                ext_third_cache,
                ext_t0_cache,
                ext_t1_cache,
                ext_fee_t0_cache,
                ext_fee_t1_cache,
                ext_requires_fee_cache,
                ext_requires_pre_fee_cache,
                ext_pre_fee_meta_1_cache,
                sell_cashback_remaining,
                sell_cashback_third,
                sell_tail_0,
                sell_tail_1,
                sell_fee_tail_0,
                sell_fee_tail_1,
                refresh_requires_fee_tail,
                sell_requires_pre_fee_metas,
                sell_pre_fee_meta_1,
                sell_layout_ready_from_refresh,
            );

            let (base_reserve, quote_reserve) =
                match resolve_pump_amm_reserves_for_ensure_discovery(
                    request_id,
                    &base_mint_str,
                    &pool_address_str,
                    existing.as_ref(),
                    &accounts,
                    dex,
                    force_refresh,
                )
                .await
                {
                    Ok(pair) => pair,
                    Err(msg) => {
                        warn!(
                            request_id = %request_id,
                            base_mint = %base_mint_str,
                            pool = %pool_address_str,
                            error = %msg,
                            "I-24d Discovery: terminal outcome error (reserve hydration required)"
                        );
                        if let Some(nats) = host.nats() {
                            publish_control_response(
                                nats,
                                host.run_id(),
                                request_id,
                                ControlResponseStatus::Error,
                                Some(pool_address_str),
                                Some(msg),
                            )
                            .await;
                        }
                        return;
                    }
                };

            let state = CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint,
                pool_base_token_account: accounts[4],
                pool_quote_token_account: accounts[5],
                base_reserve: Some(base_reserve),
                quote_reserve: Some(quote_reserve),
                pool_accounts: accounts.clone(),
                creator: creator_opt,
            });
            let publish_slot = resolve_cold_path_publish_slot(0);
            if publish_slot == 0 {
                warn!(
                    request_id = %request_id,
                    pool = %pool_address_str,
                    "EnsurePumpAmmPoolAccounts: no publish slot watermark (Geyser head zero)"
                );
            }
            host.live_pool_cache()
                .upsert(pool_address, state, publish_slot);
            if force_refresh {
                host.live_pool_cache()
                    .set_pump_amm_sell_layout_authoritative(
                        &pool_address,
                        sell_flag_merged,
                        third_merged,
                        tail0_merged,
                        tail1_merged,
                        fee_tail0_merged,
                        fee_tail1_merged,
                        requires_fee_tail_merged,
                        requires_pre_fee_metas_merged,
                        pre_fee_meta_1_merged,
                    );
            } else {
                host.live_pool_cache().merge_pump_amm_sell_extended_layout(
                    &pool_address,
                    sell_flag_merged,
                    third_merged,
                    tail0_merged,
                    tail1_merged,
                    fee_tail0_merged,
                    fee_tail1_merged,
                    requires_fee_tail_merged,
                    requires_pre_fee_metas_merged,
                    pre_fee_meta_1_merged,
                );
            }
            host.live_pool_cache()
                .set_pump_amm_sell_layout_ready(&pool_address, sell_layout_ready);
            host.live_pool_cache()
                .set_pump_amm_pool_accounts_readiness_authoritative(pool_address, dex_readiness);

            let layout_generation = if force_refresh {
                host.live_pool_cache()
                    .bump_pump_amm_layout_generation(&pool_address)
            } else {
                host.live_pool_cache()
                    .pump_amm_layout_generation(&pool_address)
            };

            // Publish JetStream PoolCacheUpdate (authoritative SSOT).
            // Reply Ok ONLY when JetStream write succeeds (I-24a).
            let jetstream_ok = if let Some(nats) = host.nats() {
                let mut pool_update = PoolCacheUpdate::new_pool_discovered(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    pool_address_str.clone(),
                    "pump_amm".to_string(),
                    base_mint_str.clone(),
                    quote_mint_str,
                    base_reserve,
                    quote_reserve,
                    None,
                    publish_slot,
                );
                let mut meta = std::collections::HashMap::new();
                let accounts_str: Vec<String> = accounts.iter().map(|p| p.to_string()).collect();
                meta.insert("pool_accounts".to_string(), accounts_str.join(","));
                meta.insert(
                    "pump_amm_sell_cashback_remaining".to_string(),
                    sell_flag_merged.to_string(),
                );
                meta.insert(
                    "pump_amm_sell_layout_ready".to_string(),
                    sell_layout_ready.to_string(),
                );
                if force_refresh {
                    meta.insert(
                        "pump_amm_sell_layout_authoritative".to_string(),
                        "true".to_string(),
                    );
                }
                meta.insert(
                    "pump_amm_layout_generation".to_string(),
                    layout_generation.to_string(),
                );
                if requires_fee_tail_merged {
                    meta.insert(
                        "pump_amm_sell_requires_fee_tail".to_string(),
                        "true".to_string(),
                    );
                }
                if requires_pre_fee_metas_merged {
                    meta.insert(
                        "pump_amm_sell_requires_pre_fee_metas".to_string(),
                        "true".to_string(),
                    );
                }
                if let Some(pk) = pre_fee_meta_1_merged {
                    meta.insert("pump_amm_sell_pre_fee_meta_1".to_string(), pk.to_string());
                }
                if let Some(pk) = third_merged {
                    meta.insert(
                        "pump_amm_sell_cashback_third_meta".to_string(),
                        pk.to_string(),
                    );
                }
                if let Some(pk) = tail0_merged {
                    meta.insert("pump_amm_sell_extended_tail_0".to_string(), pk.to_string());
                }
                if let Some(pk) = tail1_merged {
                    meta.insert("pump_amm_sell_extended_tail_1".to_string(), pk.to_string());
                }
                if let Some(pk) = fee_tail0_merged {
                    meta.insert(
                        "pump_amm_sell_extended_fee_tail_0".to_string(),
                        pk.to_string(),
                    );
                }
                if let Some(pk) = fee_tail1_merged {
                    meta.insert(
                        "pump_amm_sell_extended_fee_tail_1".to_string(),
                        pk.to_string(),
                    );
                }
                if let Some(creator) = creator_opt {
                    meta.insert("creator".to_string(), creator.to_string());
                }
                pool_update.metadata = Some(meta);
                pool_update.set_dex_readiness_in_metadata(dex_readiness);
                let subject = pool_subject(&pool_address_str);
                match nats.jetstream_publish(&subject, &pool_update).await {
                    Ok(true) => {
                        let sell_ix_account_count = pump_amm_inferred_sell_ix_account_count(
                            requires_pre_fee_metas_merged,
                            requires_fee_tail_merged,
                            sell_flag_merged,
                        );
                        info!(
                            pool = %pool_address_str,
                            base_mint = %base_mint_str,
                            force_refresh,
                            sell_requires_pre_fee_metas = requires_pre_fee_metas_merged,
                            sell_pre_fee_meta_1 = ?pre_fee_meta_1_merged,
                            sell_ix_account_count,
                            sell_layout_ready,
                            "EnsurePumpAmmPoolAccounts: Published PoolCacheUpdate to JetStream"
                        );
                        true
                    }
                    Ok(false) => {
                        warn!(
                            "EnsurePumpAmmPoolAccounts: JetStream publish failed (timeout or drop)"
                        );
                        false
                    }
                    Err(e) => {
                        warn!(error = %e, "EnsurePumpAmmPoolAccounts: Failed to publish PoolCacheUpdate to JetStream");
                        false
                    }
                }
            } else {
                false
            };

            if let Some(nats) = host.nats() {
                let (status, message) =
                    super::pump_layout::pump_amm_control_response_for_ensure_publish(
                        force_refresh,
                        jetstream_ok,
                        sell_layout_ready,
                    );
                if force_refresh && jetstream_ok && !sell_layout_ready {
                    log_pump_amm_scope49_force_refresh_sell_layout_failure(
                        request_id,
                        &pool_address_str,
                        &base_mint_str,
                        pump_amm_force_refresh_sell_diag.as_ref(),
                    );
                }
                match (jetstream_ok, force_refresh, sell_layout_ready) {
                    (true, _, true) => {
                        info!(
                            request_id = %request_id,
                            base_mint = %base_mint_str,
                            pool_address = %pool_address_str,
                            rpc_global_scan = "no",
                            control_response = "pending_publish",
                            "I-24d Discovery: terminal outcome ok"
                        );
                    }
                    (true, true, false) => {
                        warn!(
                            request_id = %request_id,
                            base_mint = %base_mint_str,
                            pool_address = %pool_address_str,
                            sell_cashback_remaining = sell_flag_merged,
                            sell_cashback_third_meta = ?third_merged,
                            sell_extended_tail_0 = ?tail0_merged,
                            sell_extended_tail_1 = ?tail1_merged,
                            "I-24d Discovery: force_refresh result published as Partial (authoritative SELL layout unresolved)"
                        );
                    }
                    (true, false, false) => {
                        info!(
                            request_id = %request_id,
                            base_mint = %base_mint_str,
                            pool_address = %pool_address_str,
                            sell_cashback_remaining = sell_flag_merged,
                            sell_cashback_third_meta = ?third_merged,
                            sell_extended_tail_0 = ?tail0_merged,
                            sell_extended_tail_1 = ?tail1_merged,
                            "I-24d Discovery: terminal outcome ok (non-force-refresh publish may remain Partial)"
                        );
                    }
                    (false, _, _) => {
                        warn!(
                            request_id = %request_id,
                            base_mint = %base_mint_str,
                            "I-24d Discovery: terminal outcome error (JetStream publish failed)"
                        );
                    }
                }
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    status,
                    Some(pool_address_str),
                    message,
                )
                .await;
            }
        }
        Ok(Some(_)) => {
            warn!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                outcome = "error",
                reason = "pool_accounts_incomplete",
                "I-24d Discovery: terminal outcome error (pool_accounts incomplete <14)"
            );
            if let Some(nats) = host.nats() {
                publish_control_response(
                    nats,
                    host.run_id(),
                    request_id,
                    ControlResponseStatus::Error,
                    None,
                    Some("pool_accounts incomplete".to_string()),
                )
                .await;
            }
        }
        Ok(None) => {
            info!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                outcome = "not_found",
                "I-24d Discovery: terminal outcome not_found"
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
        }
        Err(e) => {
            warn!(
                request_id = %request_id,
                base_mint = %base_mint_str,
                outcome = "error",
                error = %e,
                "I-24d Discovery: terminal outcome error (discovery failed)"
            );
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
        }
    }
}
