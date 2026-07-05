//! Phase 5b: md-sidefx job handlers (cache + NATS only; no md-state register from parse).

use super::host::{MarketEventCorePublishTrace, SidefxWorkerHost};
use super::pool_publish::{
    meteora_cpmm_onchain_mints_for_pool_cache_update, meteora_cpmm_vaults_for_pool_cache_update,
    meteora_dlmm_metadata_for_pool_cache_update, orca_metadata_for_pool_cache_update,
    pool_cache_balance_fields_from_state, pump_amm_sell_layout_publish_state,
    raydium_cpmm_readiness_for_pool_cache_update, raydium_cpmm_vaults_for_pool_cache_update,
};
use super::worker::{DlmmPoolStateSignal, MdSidefxBurstScratch, MdSidefxCommand};
use crate::execution::live_pool_cache::{
    meteora_cpmm_readiness_for_pool_cache_update, meteora_dlmm_readiness_for_pool_cache_update,
    orca_readiness_for_pool_cache_update, parse_pool_account,
    raydium_amm_readiness_for_pool_cache_update, CachedPoolState, PumpFunState,
};
use crate::ipc::{
    DexPoolReadiness, MarketEvent, MarketEventKind, PoolCacheUpdate, NATIVE_SOL_MINT,
    POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY, POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY,
    POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY,
};
use crate::metrics::{
    inc_market_data_devwallet_bonding_path_total, inc_market_data_devwallet_tx_published_total,
    inc_market_data_pool_state_publish_skipped_balance_unchanged_total,
    record_market_data_bonding_curve_grpc_to_devwallet_ms,
    record_market_data_pool_mint_map_to_devwallet_ms, MarketDataLatencySegment,
};
use crate::nats::jetstream::pool_subject;
use crate::solana::dex_parser::DexType;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::time::Instant;
use tracing::{debug, info, warn};

fn sidefx_host_enqueue_jetstream<T: serde::Serialize>(
    host: &dyn SidefxWorkerHost,
    subject: String,
    payload: &T,
    log_fail: &'static str,
    bump_market_events_published_total: bool,
) {
    let payload = match serde_json::to_value(payload) {
        Ok(v) => v,
        Err(e) => {
            warn!(
                error = %e,
                msg = log_fail,
                "md-sidefx: failed to serialize JetStream payload"
            );
            return;
        }
    };
    host.enqueue_jetstream(
        subject,
        payload,
        log_fail,
        bump_market_events_published_total,
    );
}

/// Build PoolStateUpdate from MASTER LivePoolCache (Geyser-only; no RPC).
fn md_sidefx_build_pool_state_update_from_cache(
    host: &dyn SidefxWorkerHost,
    run_id: &str,
    pool_pubkey: &Pubkey,
    slot: u64,
) -> Option<MarketEvent> {
    let state = host.live_pool_cache().get(pool_pubkey)?;
    let (base_mint, quote_mint, reserve_base, reserve_quote, dex) =
        pool_cache_balance_fields_from_state(&state)?;
    let (active_id, bin_step) = match &state {
        CachedPoolState::Meteora(s) => (Some(s.active_id), Some(s.bin_step)),
        _ => (None, None),
    };
    Some(MarketEvent::new(
        "market-data",
        host.build_version(),
        run_id,
        host.next_event_id(),
        "geyser_dlmm_cache",
        Some(slot),
        MarketEventKind::PoolStateUpdate {
            pool_address: pool_pubkey.to_string(),
            dex: dex.to_string(),
            reserve_base,
            reserve_quote,
            base_mint: base_mint.to_string(),
            quote_mint: quote_mint.to_string(),
            update_slot: slot,
            active_id,
            bin_step,
        },
    ))
}

fn md_sidefx_publish_pool_state_update_from_cache(
    host: &dyn SidefxWorkerHost,
    run_id: &str,
    pool_pubkey: &Pubkey,
    slot: u64,
    grpc_recv_at: Instant,
) -> bool {
    let Some(state_event) =
        md_sidefx_build_pool_state_update_from_cache(host, run_id, pool_pubkey, slot)
    else {
        return false;
    };
    host.write_market_event_jsonl(&state_event);
    if host.nats_enabled() {
        host.enqueue_core_market_event(
            state_event,
            Some(MarketEventCorePublishTrace {
                recv_at: grpc_recv_at,
                cold_path: false,
                segment: MarketDataLatencySegment::Other,
            }),
        )
    } else {
        false
    }
}

pub fn md_sidefx_flush_pending_dlmm_pool_state_publishes(
    host: &dyn SidefxWorkerHost,
    scratch: &mut MdSidefxBurstScratch,
) {
    let signals: Vec<(Pubkey, DlmmPoolStateSignal)> = scratch.drain_dlmm_pool_state_signals();
    for (pool, signal) in signals {
        if !host.is_hot_pool(&pool) {
            continue;
        }
        let _ = md_sidefx_publish_pool_state_update_from_cache(
            host,
            &signal.run_id,
            &pool,
            signal.slot,
            signal.grpc_recv_at,
        );
    }
}

pub fn md_sidefx_process_dlmm_pool_state_publish_signal(
    _host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
    scratch: &mut MdSidefxBurstScratch,
) {
    let MdSidefxCommand::DlmmPoolStatePublishSignal {
        run_id,
        pool_address,
        slot,
        grpc_recv_at,
    } = job
    else {
        return;
    };
    scratch.note_dlmm_pool_state_signal(*pool_address, run_id, *slot, *grpc_recv_at);
}

pub fn sidefx_maybe_emit_dev_wallet_after_pool_mint_map(
    host: &dyn SidefxWorkerHost,
    run_id: &str,
    pool: &Pubkey,
    mint: &str,
    slot: Option<u64>,
    tx_grpc_recv_at: Instant,
    creator_override: Option<&str>,
) -> bool {
    let pool_str = pool.to_string();
    let creator_str = if let Some(c) = creator_override.filter(|s| !s.is_empty()) {
        c.to_string()
    } else if let Some(s) = host
        .pool_creator_cache_get(&pool_str)
        .filter(|s| !s.is_empty())
    {
        s
    } else {
        match host.live_pool_cache().get_pumpfun_creator(pool) {
            Some(pk) if pk != Pubkey::default() => pk.to_string(),
            _ => return false,
        }
    };
    host.pool_creator_cache_insert(pool_str.clone(), creator_str.clone());
    let existing = host.creator_cache_insert_returning_old(mint.to_string(), creator_str.clone());
    let should_emit = match &existing {
        None => true,
        Some(old) if old != &creator_str => {
            warn!(
                mint = %mint,
                pool = %pool_str,
                old_creator = %old,
                new_creator = %creator_str,
                "FIX-22: Creator mismatch on TX fast-path — pool_creator_cache overwrites stale value"
            );
            true
        }
        _ => false,
    };
    if !should_emit {
        return false;
    }
    let publish_at = Instant::now();
    record_market_data_pool_mint_map_to_devwallet_ms(tx_grpc_recv_at, publish_at);
    inc_market_data_devwallet_tx_published_total();
    info!(
        mint = %mint,
        pool = %pool_str,
        creator = %creator_str,
        corrected = existing.is_some(),
        had_override = creator_override.is_some(),
        "DevWalletIdentified from TX path after pool_mint_map (P1 creator)"
    );
    let dev_event = MarketEvent::new(
        "market-data",
        host.build_version(),
        run_id,
        host.next_event_id(),
        "geyser",
        slot,
        MarketEventKind::DevWalletIdentified {
            mint: mint.to_string(),
            dev_wallet: creator_str.clone(),
            supply_percentage: 0.0,
        },
    );
    host.write_market_event_jsonl(&dev_event);
    if host.nats_enabled() {
        host.enqueue_core_market_event(
            dev_event,
            Some(MarketEventCorePublishTrace {
                recv_at: tx_grpc_recv_at,
                cold_path: false,
                segment: MarketDataLatencySegment::Other,
            }),
        );
    }
    true
}

pub fn md_sidefx_process_pump_fun_pool_mint_map(
    host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
) {
    let MdSidefxCommand::PumpFunPoolMintMapInsert {
        run_id,
        pool_address,
        mint_str,
        slot,
        tx_grpc_recv_at,
        creator_override,
    } = job
    else {
        return;
    };
    {
        host.pool_mint_map_insert(pool_address.to_string(), mint_str.clone());
        host.high_priority_bonding_curves_insert(*pool_address);
    }
    let override_str = creator_override.as_ref().map(|pk| pk.to_string());
    sidefx_maybe_emit_dev_wallet_after_pool_mint_map(
        host,
        run_id,
        pool_address,
        mint_str,
        *slot,
        *tx_grpc_recv_at,
        override_str.as_deref(),
    );
}

pub fn md_sidefx_process_pump_fun_dev_wallet_from_pool_created(
    host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
) {
    let MdSidefxCommand::PumpFunDevWalletFromPoolCreated {
        run_id,
        pool_address,
        base_mint,
        creator,
        slot,
        tx_geyser_recv_at,
    } = job
    else {
        return;
    };
    let pool_str = pool_address.to_string();
    let creator_str = creator.to_string();
    host.pool_creator_cache_insert(pool_str.clone(), creator_str.clone());
    host.creator_cache_set(base_mint.to_string(), creator_str.clone());
    debug!(
        mint = %base_mint,
        pool = %pool_str,
        creator = %creator,
        "Cached PumpFun creator for Trade enrichment (PoolCreated TX path)"
    );
    inc_market_data_devwallet_tx_published_total();
    let dev_event = MarketEvent::new(
        "market-data",
        host.build_version(),
        run_id,
        host.next_event_id(),
        "geyser",
        Some(*slot),
        MarketEventKind::DevWalletIdentified {
            mint: base_mint.to_string(),
            dev_wallet: creator.to_string(),
            supply_percentage: 0.0,
        },
    );
    host.write_market_event_jsonl(&dev_event);
    if host.nats_enabled() {
        host.enqueue_core_market_event(
            dev_event,
            Some(MarketEventCorePublishTrace {
                recv_at: *tx_geyser_recv_at,
                cold_path: false,
                segment: MarketDataLatencySegment::Other,
            }),
        );
    }
}

pub fn md_sidefx_process_pump_amm_create_pool(host: &dyn SidefxWorkerHost, job: &MdSidefxCommand) {
    let MdSidefxCommand::PumpAmmCreatePoolObserved {
        run_id,
        pool_address,
        base_mint,
        quote_mint,
        slot,
        tx_geyser_recv_at,
    } = job
    else {
        return;
    };
    let is_new_pool = host.known_pump_amm_pools_insert(*pool_address);
    if !is_new_pool {
        return;
    }
    info!(
        pool = %pool_address,
        base_mint = %base_mint,
        "pump_amm pool observed via create_pool — PoolCreated only (DexPoolAccounts/cache deferred to verified path)"
    );
    let pool_created_event = MarketEvent::new(
        "market-data",
        host.build_version(),
        run_id,
        host.next_event_id(),
        "geyser_create_pool",
        Some(*slot),
        MarketEventKind::PoolCreated {
            pool_address: pool_address.to_string(),
            base_mint: base_mint.clone(),
            quote_mint: quote_mint.clone(),
            dex: DexType::PumpFunAmm.to_string(),
            initial_liquidity_sol: None,
        },
    );
    host.write_market_event_jsonl(&pool_created_event);
    if host.nats_enabled() {
        host.enqueue_core_market_event(
            pool_created_event,
            Some(MarketEventCorePublishTrace {
                recv_at: *tx_geyser_recv_at,
                cold_path: false,
                segment: MarketDataLatencySegment::PoolCreated,
            }),
        );
    }
    host.pool_mint_map_insert(pool_address.to_string(), base_mint.clone());
}

#[allow(clippy::type_complexity)]
pub fn md_sidefx_process_pump_amm_trade(host: &dyn SidefxWorkerHost, job: &MdSidefxCommand) {
    let MdSidefxCommand::PumpAmmTradeWithAccounts {
        run_id,
        pool_address,
        base_mint_pk,
        slot,
        is_buy,
        pool_accounts,
        pump_amm_sell_requires_cashback_remaining,
        pump_amm_sell_cashback_third_meta,
        pump_amm_sell_extended_tail_0,
        pump_amm_sell_extended_tail_1,
        pump_amm_sell_extended_fee_tail_0,
        pump_amm_sell_extended_fee_tail_1,
        pump_amm_sell_requires_fee_tail,
        pump_amm_sell_requires_pre_fee_metas,
        pump_amm_sell_pre_fee_meta_1,
        tx_geyser_recv_at,
    } = job
    else {
        return;
    };
    let filter_opt_pk = |p: Option<Pubkey>| p.filter(|pk| *pk != Pubkey::default());
    let this_tx_tail = filter_opt_pk(*pump_amm_sell_cashback_third_meta)
        .or(filter_opt_pk(*pump_amm_sell_extended_tail_0))
        .or(filter_opt_pk(*pump_amm_sell_extended_tail_1))
        .or(filter_opt_pk(*pump_amm_sell_extended_fee_tail_0))
        .or(filter_opt_pk(*pump_amm_sell_extended_fee_tail_1))
        .or(filter_opt_pk(*pump_amm_sell_pre_fee_meta_1));
    let this_tx_defines_sell_layout =
        *pump_amm_sell_requires_cashback_remaining || this_tx_tail.is_some();
    let (ext_flag_prior, ext_third_prior, ext_t0_prior, ext_t1_prior) = host
        .live_pool_cache()
        .pump_amm_sell_extended_layout(pool_address);
    let (ext_fee_t0_prior, ext_fee_t1_prior) = host
        .live_pool_cache()
        .pump_amm_sell_fee_tail_layout(pool_address);
    let ext_requires_fee_tail_prior = host
        .live_pool_cache()
        .pump_amm_sell_requires_fee_tail(pool_address);
    let ext_requires_pre_fee_prior = host
        .live_pool_cache()
        .pump_amm_sell_requires_pre_fee_metas(pool_address);
    let ext_pre_fee_meta_1_prior = host
        .live_pool_cache()
        .pump_amm_sell_pre_fee_meta_1(pool_address);
    let merge_requires_fee_tail = if this_tx_defines_sell_layout {
        *pump_amm_sell_requires_fee_tail
    } else {
        ext_requires_fee_tail_prior || *pump_amm_sell_requires_fee_tail
    };
    let merge_requires = if this_tx_defines_sell_layout {
        *pump_amm_sell_requires_cashback_remaining
    } else {
        *pump_amm_sell_requires_cashback_remaining || ext_flag_prior
    };
    let merge_third = if this_tx_defines_sell_layout {
        filter_opt_pk(*pump_amm_sell_cashback_third_meta)
    } else {
        filter_opt_pk(*pump_amm_sell_cashback_third_meta).or(ext_third_prior)
    };
    let merge_t0 = if this_tx_defines_sell_layout {
        filter_opt_pk(*pump_amm_sell_extended_tail_0)
    } else {
        filter_opt_pk(*pump_amm_sell_extended_tail_0).or(ext_t0_prior)
    };
    let merge_t1 = if this_tx_defines_sell_layout {
        filter_opt_pk(*pump_amm_sell_extended_tail_1)
    } else {
        filter_opt_pk(*pump_amm_sell_extended_tail_1).or(ext_t1_prior)
    };
    let merge_fee_t0 = if this_tx_defines_sell_layout {
        filter_opt_pk(*pump_amm_sell_extended_fee_tail_0)
    } else {
        filter_opt_pk(*pump_amm_sell_extended_fee_tail_0).or(ext_fee_t0_prior)
    };
    let merge_fee_t1 = if this_tx_defines_sell_layout {
        filter_opt_pk(*pump_amm_sell_extended_fee_tail_1)
    } else {
        filter_opt_pk(*pump_amm_sell_extended_fee_tail_1).or(ext_fee_t1_prior)
    };
    let merge_requires_pre_fee_metas = if this_tx_defines_sell_layout {
        *pump_amm_sell_requires_pre_fee_metas
    } else {
        ext_requires_pre_fee_prior || *pump_amm_sell_requires_pre_fee_metas
    };
    let merge_pre_fee_meta_1 = if this_tx_defines_sell_layout {
        filter_opt_pk(*pump_amm_sell_pre_fee_meta_1)
    } else {
        filter_opt_pk(*pump_amm_sell_pre_fee_meta_1).or(ext_pre_fee_meta_1_prior)
    };
    if merge_requires
        || merge_third.is_some()
        || merge_t0.is_some()
        || merge_t1.is_some()
        || merge_fee_t0.is_some()
        || merge_fee_t1.is_some()
        || merge_requires_pre_fee_metas
        || merge_pre_fee_meta_1.is_some()
    {
        host.live_pool_cache().merge_pump_amm_sell_extended_layout(
            pool_address,
            merge_requires,
            merge_third,
            merge_t0,
            merge_t1,
            merge_fee_t0,
            merge_fee_t1,
            merge_requires_fee_tail,
            merge_requires_pre_fee_metas,
            merge_pre_fee_meta_1,
        );
    }
    let base_mint = pool_accounts
        .get(2)
        .map(|p| p.to_string())
        .unwrap_or_default();
    let quote_mint = pool_accounts
        .get(3)
        .map(|p| p.to_string())
        .unwrap_or_default();

    let is_first_trade = host.known_pump_amm_pools_insert(*pool_address);

    if is_first_trade {
        info!(
            pool = %pool_address,
            base_mint = %base_mint_pk,
            "pump_amm pool discovered via first trade - emitting PoolCreated + DexPoolAccounts"
        );

        let pool_created_event = MarketEvent::new(
            "market-data",
            host.build_version(),
            run_id,
            host.next_event_id(),
            "geyser_first_trade",
            Some(*slot),
            MarketEventKind::PoolCreated {
                pool_address: pool_address.to_string(),
                base_mint: base_mint.clone(),
                quote_mint: quote_mint.clone(),
                dex: DexType::PumpFunAmm.to_string(),
                initial_liquidity_sol: None,
            },
        );

        host.write_market_event_jsonl(&pool_created_event);

        if host.nats_enabled() {
            host.enqueue_core_market_event(
                pool_created_event,
                Some(MarketEventCorePublishTrace {
                    recv_at: *tx_geyser_recv_at,
                    cold_path: false,
                    segment: MarketDataLatencySegment::PoolCreated,
                }),
            );
        }
    }
    let accounts_event = MarketEvent::new(
        "market-data",
        host.build_version(),
        run_id,
        host.next_event_id(),
        "geyser",
        Some(*slot),
        MarketEventKind::DexPoolAccounts {
            dex: DexType::PumpFunAmm.to_string(),
            pool_address: pool_address.to_string(),
            base_mint: base_mint.clone(),
            quote_mint: quote_mint.clone(),
            accounts: pool_accounts.iter().map(|p| p.to_string()).collect(),
        },
    );

    host.write_market_event_jsonl(&accounts_event);

    if host.nats_enabled() {
        host.enqueue_core_market_event(
            accounts_event,
            Some(MarketEventCorePublishTrace {
                recv_at: *tx_geyser_recv_at,
                cold_path: false,
                segment: MarketDataLatencySegment::Other,
            }),
        );
    }
    if pool_accounts.len() >= 14 {
        host.live_pool_cache()
            .set_pump_amm_pool_accounts(pool_address, pool_accounts.clone());
        let (ext_flag, ext_third, ext_t0, ext_t1) = host
            .live_pool_cache()
            .pump_amm_sell_extended_layout(pool_address);
        let (ext_fee_t0, ext_fee_t1) = host
            .live_pool_cache()
            .pump_amm_sell_fee_tail_layout(pool_address);
        let ext_requires_fee_tail = host
            .live_pool_cache()
            .pump_amm_sell_requires_fee_tail(pool_address);
        let ext_requires_pre_fee = host
            .live_pool_cache()
            .pump_amm_sell_requires_pre_fee_metas(pool_address);
        let merged_requires_fee_tail = if this_tx_defines_sell_layout {
            *pump_amm_sell_requires_fee_tail
        } else {
            ext_requires_fee_tail || *pump_amm_sell_requires_fee_tail
        };
        let merged_requires_pre_fee_metas = if this_tx_defines_sell_layout {
            *pump_amm_sell_requires_pre_fee_metas
        } else {
            ext_requires_pre_fee || *pump_amm_sell_requires_pre_fee_metas
        };
        let merged_pre_fee_meta_1 = if this_tx_defines_sell_layout {
            filter_opt_pk(*pump_amm_sell_pre_fee_meta_1)
        } else {
            host.live_pool_cache()
                .pump_amm_sell_pre_fee_meta_1(pool_address)
                .or(*pump_amm_sell_pre_fee_meta_1)
                .and_then(|p| filter_opt_pk(Some(p)))
        };
        let merged_flag = if this_tx_defines_sell_layout {
            *pump_amm_sell_requires_cashback_remaining
        } else {
            ext_flag || *pump_amm_sell_requires_cashback_remaining
        };
        let merged_third = if this_tx_defines_sell_layout {
            filter_opt_pk(*pump_amm_sell_cashback_third_meta)
        } else {
            ext_third
                .or(*pump_amm_sell_cashback_third_meta)
                .and_then(|p| filter_opt_pk(Some(p)))
        };
        let merged_t0 = if this_tx_defines_sell_layout {
            filter_opt_pk(*pump_amm_sell_extended_tail_0)
        } else {
            ext_t0
                .or(*pump_amm_sell_extended_tail_0)
                .and_then(|p| filter_opt_pk(Some(p)))
        };
        let merged_t1 = if this_tx_defines_sell_layout {
            filter_opt_pk(*pump_amm_sell_extended_tail_1)
        } else {
            ext_t1
                .or(*pump_amm_sell_extended_tail_1)
                .and_then(|p| filter_opt_pk(Some(p)))
        };
        let merged_fee_t0 = if this_tx_defines_sell_layout {
            filter_opt_pk(*pump_amm_sell_extended_fee_tail_0)
        } else {
            ext_fee_t0
                .or(*pump_amm_sell_extended_fee_tail_0)
                .and_then(|p| filter_opt_pk(Some(p)))
        };
        let merged_fee_t1 = if this_tx_defines_sell_layout {
            filter_opt_pk(*pump_amm_sell_extended_fee_tail_1)
        } else {
            ext_fee_t1
                .or(*pump_amm_sell_extended_fee_tail_1)
                .and_then(|p| filter_opt_pk(Some(p)))
        };
        let (sell_layout_ready, dex_readiness) = pump_amm_sell_layout_publish_state(
            merged_flag,
            merged_third,
            merged_t0,
            merged_t1,
            merged_fee_t0,
            merged_fee_t1,
            merged_requires_fee_tail,
            merged_requires_pre_fee_metas,
            merged_pre_fee_meta_1,
            true,
        );
        host.live_pool_cache()
            .set_pump_amm_sell_layout_ready(pool_address, sell_layout_ready);
        if !*is_buy {
            crate::metrics::record_pump_amm_geyser_sell_parsed();
            if sell_layout_ready {
                host.live_pool_cache()
                    .set_pump_amm_sell_layout_authoritative(
                        pool_address,
                        merged_flag,
                        merged_third,
                        merged_t0,
                        merged_t1,
                        merged_fee_t0,
                        merged_fee_t1,
                        merged_requires_fee_tail,
                        merged_requires_pre_fee_metas,
                        merged_pre_fee_meta_1,
                    );
                crate::metrics::record_pump_amm_geyser_sell_layout_ready();
                info!(
                    pool = %pool_address,
                    base_mint = %base_mint_pk,
                    slot = *slot,
                    "pump_amm: Geyser SELL set sell_layout_ready (authoritative extended layout)"
                );
            }
        }
        host.live_pool_cache()
            .set_pump_amm_pool_accounts_readiness_authoritative(*pool_address, dex_readiness);

        if host.nats_enabled() {
            let (pub_base_reserve, pub_quote_reserve) =
                match host.live_pool_cache().get(pool_address) {
                    Some(CachedPoolState::PumpAmm(ref s)) => {
                        (s.base_reserve.unwrap_or(0), s.quote_reserve.unwrap_or(0))
                    }
                    _ => (0, 0),
                };
            let mut pool_update = PoolCacheUpdate::new_pool_discovered(
                "market-data",
                host.build_version(),
                run_id,
                pool_address.to_string(),
                "pump_amm".to_string(),
                base_mint.clone(),
                quote_mint.clone(),
                pub_base_reserve,
                pub_quote_reserve,
                None,
                *slot,
            );
            let mut meta = std::collections::HashMap::new();
            let accounts_str: Vec<String> = pool_accounts.iter().map(|p| p.to_string()).collect();
            meta.insert("pool_accounts".to_string(), accounts_str.join(","));
            meta.insert(
                "pump_amm_sell_cashback_remaining".to_string(),
                merged_flag.to_string(),
            );
            meta.insert(
                "pump_amm_sell_layout_ready".to_string(),
                sell_layout_ready.to_string(),
            );
            if !*is_buy && sell_layout_ready && this_tx_defines_sell_layout {
                meta.insert(
                    "pump_amm_sell_layout_authoritative".to_string(),
                    "true".to_string(),
                );
            }
            if merged_requires_fee_tail {
                meta.insert(
                    "pump_amm_sell_requires_fee_tail".to_string(),
                    "true".to_string(),
                );
            }
            if merged_requires_pre_fee_metas {
                meta.insert(
                    "pump_amm_sell_requires_pre_fee_metas".to_string(),
                    "true".to_string(),
                );
            }
            if let Some(pk) = merged_pre_fee_meta_1 {
                meta.insert("pump_amm_sell_pre_fee_meta_1".to_string(), pk.to_string());
            }
            if let Some(pk) = merged_third {
                meta.insert(
                    "pump_amm_sell_cashback_third_meta".to_string(),
                    pk.to_string(),
                );
            }
            if let Some(pk) = merged_t0 {
                meta.insert("pump_amm_sell_extended_tail_0".to_string(), pk.to_string());
            }
            if let Some(pk) = merged_t1 {
                meta.insert("pump_amm_sell_extended_tail_1".to_string(), pk.to_string());
            }
            if !*is_buy {
                if let Some(pk) = merged_fee_t0 {
                    meta.insert(
                        "pump_amm_sell_extended_fee_tail_0".to_string(),
                        pk.to_string(),
                    );
                }
                if let Some(pk) = merged_fee_t1 {
                    meta.insert(
                        "pump_amm_sell_extended_fee_tail_1".to_string(),
                        pk.to_string(),
                    );
                }
            }
            pool_update.metadata = Some(meta);
            pool_update.set_dex_readiness_in_metadata(dex_readiness);
            let subject = pool_subject(&pool_address.to_string());
            sidefx_host_enqueue_jetstream(
                host,
                subject,
                &pool_update,
                "FIX-33 pump_amm pool_accounts PoolCacheUpdate (trade)",
                false,
            );
        }
    }
}

pub fn md_sidefx_process_generic_dex_first_trade(
    host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
) {
    let MdSidefxCommand::GenericDexFirstTradeAccounts {
        run_id,
        pool_address,
        mint,
        quote_mint,
        dex,
        pool_accounts,
        slot,
        tx_geyser_recv_at,
    } = job
    else {
        return;
    };
    if matches!(dex, DexType::PumpFunAmm) {
        return;
    }
    let is_first_trade = host.known_trade_dex_pools_insert(*pool_address);
    if !is_first_trade {
        return;
    }
    let accounts_event = MarketEvent::new(
        "market-data",
        host.build_version(),
        run_id,
        host.next_event_id(),
        "geyser_first_trade",
        Some(*slot),
        MarketEventKind::DexPoolAccounts {
            dex: dex.to_string(),
            pool_address: pool_address.to_string(),
            base_mint: mint.to_string(),
            quote_mint: quote_mint.to_string(),
            accounts: pool_accounts.iter().map(|p| p.to_string()).collect(),
        },
    );

    host.write_market_event_jsonl(&accounts_event);

    if host.nats_enabled() {
        host.enqueue_core_market_event(
            accounts_event,
            Some(MarketEventCorePublishTrace {
                recv_at: *tx_geyser_recv_at,
                cold_path: false,
                segment: MarketDataLatencySegment::Other,
            }),
        );
    }
}

pub fn md_sidefx_process_bonding_curve(host: &dyn SidefxWorkerHost, job: &MdSidefxCommand) {
    let MdSidefxCommand::BondingCurveDevWallet {
        run_id,
        pool_address,
        creator,
        slot,
        grpc_recv_at,
        virtual_token_reserves,
        virtual_sol_reserves,
        real_token_reserves,
        real_sol_reserves,
        complete,
        cashback_enabled,
    } = job
    else {
        return;
    };
    let pool_str = pool_address.to_string();
    let creator_str = creator.to_string();

    host.pool_creator_cache_insert(pool_str.clone(), creator_str.clone());

    let mint_opt = host.pool_mint_map_get(&pool_str);
    if let Some(mint) = mint_opt {
        let existing = host.creator_cache_insert_returning_old(mint.clone(), creator_str.clone());

        let should_emit = match &existing {
            None => true,
            Some(old) if old != &creator_str => {
                warn!(
                    mint = %mint,
                    pool = %pool_str,
                    old_creator = %old,
                    new_creator = %creator_str,
                    "FIX-22: Creator mismatch detected — BondingCurve account data overwrites stale cache value"
                );
                true
            }
            _ => false,
        };

        if should_emit {
            info!(
                mint = %mint,
                pool = %pool_str,
                creator = %creator_str,
                corrected = existing.is_some(),
                "Creator cached from BondingCurve account update (authoritative)"
            );

            let dev_event = MarketEvent::new(
                "market-data",
                host.build_version(),
                run_id,
                host.next_event_id(),
                "geyser",
                Some(*slot),
                MarketEventKind::DevWalletIdentified {
                    mint: mint.clone(),
                    dev_wallet: creator_str.clone(),
                    supply_percentage: 0.0,
                },
            );

            host.write_market_event_jsonl(&dev_event);

            if host.nats_enabled() {
                host.enqueue_core_market_event(
                    dev_event,
                    Some(MarketEventCorePublishTrace {
                        recv_at: *grpc_recv_at,
                        cold_path: false,
                        segment: MarketDataLatencySegment::Other,
                    }),
                );
            }
            inc_market_data_devwallet_bonding_path_total();
            record_market_data_bonding_curve_grpc_to_devwallet_ms(*grpc_recv_at, Instant::now());
        }
    }

    let needs_fallback = host
        .live_pool_cache()
        .get(pool_address)
        .is_none_or(|s| !matches!(s, CachedPoolState::PumpFun(_)));
    if needs_fallback {
        let base_mint_pk = host
            .pool_mint_map_get(&pool_str)
            .and_then(|m| Pubkey::from_str(&m).ok())
            .unwrap_or_default();
        let base_mint = base_mint_pk.to_string();
        let minimal_state = CachedPoolState::PumpFun(PumpFunState {
            token_mint: base_mint_pk,
            bonding_curve: *pool_address,
            associated_bonding_curve: Pubkey::default(),
            virtual_token_reserves: *virtual_token_reserves,
            virtual_sol_reserves: *virtual_sol_reserves,
            real_token_reserves: *real_token_reserves,
            real_sol_reserves: *real_sol_reserves,
            complete: *complete,
            creator: *creator,
            cashback_enabled: *cashback_enabled,
        });
        host.live_pool_cache()
            .upsert(*pool_address, minimal_state, *slot);

        let mut pool_update = PoolCacheUpdate::new_pool_discovered(
            "market-data",
            host.build_version(),
            run_id,
            pool_str.clone(),
            "pumpfun".to_string(),
            base_mint.clone(),
            NATIVE_SOL_MINT.to_string(),
            *virtual_token_reserves,
            *virtual_sol_reserves,
            Some(0),
            *slot,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert("creator".to_string(), creator_str.clone());
        meta.insert("complete".to_string(), complete.to_string());
        meta.insert(
            "real_token_reserves".to_string(),
            real_token_reserves.to_string(),
        );
        meta.insert(
            "real_sol_reserves".to_string(),
            real_sol_reserves.to_string(),
        );
        meta.insert("cashback_enabled".to_string(), cashback_enabled.to_string());
        if let Some(d) = host.live_pool_cache().get_mint_decimals(&base_mint_pk) {
            meta.insert("base_decimals".to_string(), d.to_string());
        }
        if let Ok(sol_pk) = Pubkey::from_str(NATIVE_SOL_MINT) {
            if let Some(d) = host.live_pool_cache().get_mint_decimals(&sol_pk) {
                meta.insert("quote_decimals".to_string(), d.to_string());
            } else {
                meta.insert("quote_decimals".to_string(), "9".to_string());
            }
        }
        pool_update.metadata = Some(meta);
        pool_update.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        host.live_pool_cache()
            .merge_pumpfun_bonding_readiness(*pool_address, DexPoolReadiness::Observed);

        if host.nats_enabled() {
            let subject = pool_subject(&pool_str);
            sidefx_host_enqueue_jetstream(
                host,
                subject,
                &pool_update,
                "P2#7 BondingCurveUpdate fallback PoolCacheUpdate",
                false,
            );
            debug!(
                pool = %pool_str,
                creator = %creator_str,
                "P2#7: BondingCurveUpdate fallback PoolCacheUpdate enqueued for JetStream"
            );
        }
    }
}

pub fn md_sidefx_process_live_pool_cache_mint_decimals(
    host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
) {
    let MdSidefxCommand::LivePoolCacheMintDecimals { mint, decimals } = job else {
        return;
    };
    host.live_pool_cache().set_mint_decimals(*mint, *decimals);
}

pub fn md_sidefx_process_live_pool_cache_account_update(
    host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
    scratch: &mut MdSidefxBurstScratch,
) {
    let MdSidefxCommand::LivePoolCacheAccountUpdate {
        run_id,
        pool_pubkey,
        owner,
        account_data,
        slot,
        grpc_recv_at,
    } = job
    else {
        return;
    };
    if let Some(mut cached_state) = parse_pool_account(owner, account_data) {
        let prev_meteora_meta = host.live_pool_cache().get(pool_pubkey).and_then(|s| {
            if let CachedPoolState::Meteora(m) = s {
                Some((m.active_id, m.bin_step))
            } else {
                None
            }
        });
        // P2#7: Enrich PumpFun token_mint from pool_mint_map when parse returns default
        if let CachedPoolState::PumpFun(ref mut s) = &mut cached_state {
            if s.token_mint == Pubkey::default() {
                let pool_str = pool_pubkey.to_string();
                if let Some(mint_str) = host.pool_mint_map_get(&pool_str) {
                    if let Ok(mint_pk) = Pubkey::from_str(&mint_str) {
                        s.token_mint = mint_pk;
                        debug!(
                            pool = %pool_str,
                            mint = %mint_str,
                            "P2#7: Enriched PumpFun token_mint from pool_mint_map"
                        );
                    }
                }
            }
        }

        // Update MASTER LivePoolCache (Single Source of Truth)
        if !host
            .live_pool_cache()
            .upsert(*pool_pubkey, cached_state.clone(), *slot)
        {
            // Stale Geyser snapshot (e.g. HIGH/LOW queue reorder for same pubkey): skip downstream
            // JetStream / MarketEvent side effects so trading state cannot regress.
            return;
        }

        if let CachedPoolState::Meteora(ref s) = cached_state {
            let meta_changed = match prev_meteora_meta {
                None => host.is_hot_pool(pool_pubkey),
                Some((prev_id, prev_step)) => prev_id != s.active_id || prev_step != s.bin_step,
            };
            if meta_changed && host.is_hot_pool(pool_pubkey) {
                scratch.note_dlmm_pool_state_signal(*pool_pubkey, run_id, *slot, *grpc_recv_at);
            }
            if let Some((prev_id, _)) = prev_meteora_meta {
                if prev_id != s.active_id {
                    let _ = host.maybe_refresh_arb_dlmm_bin_window(*pool_pubkey, s.active_id);
                }
            }
        }

        // Phase1: sidefx only updates MASTER cache + JetStream; vault registration stays in md-state.
        // (No RegisterPoolVaultsFromAccount enqueue from account parse.)

        // Extract mint and reserve info from cached_state for PoolCacheUpdate
        let (base_mint, quote_mint, base_reserve, quote_reserve) = match &cached_state {
            CachedPoolState::Orca(s) => (
                s.token_mint_a,
                s.token_mint_b,
                s.vault_a_balance.unwrap_or(0),
                s.vault_b_balance.unwrap_or(0),
            ),
            CachedPoolState::RaydiumAmm(s) => (
                s.base_mint,
                s.quote_mint,
                s.coin_reserve.unwrap_or(0),
                s.pc_reserve.unwrap_or(0),
            ),
            CachedPoolState::RaydiumCpmm(s) => {
                let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
                if s.token_1_mint == sol {
                    (
                        s.token_0_mint,
                        s.token_1_mint,
                        s.reserve_0.unwrap_or(0),
                        s.reserve_1.unwrap_or(0),
                    )
                } else if s.token_0_mint == sol {
                    (
                        s.token_1_mint,
                        s.token_0_mint,
                        s.reserve_1.unwrap_or(0),
                        s.reserve_0.unwrap_or(0),
                    )
                } else {
                    (
                        s.token_0_mint,
                        s.token_1_mint,
                        s.reserve_0.unwrap_or(0),
                        s.reserve_1.unwrap_or(0),
                    )
                }
            }
            CachedPoolState::Meteora(s) => (
                s.token_x_mint,
                s.token_y_mint,
                s.reserve_x_balance.unwrap_or(0),
                s.reserve_y_balance.unwrap_or(0),
            ),
            CachedPoolState::PumpAmm(s) => {
                // Cache creator for migrated PumpFun tokens (from AMM pool account)
                if let Some(creator) = s.creator {
                    let mint_str = s.base_mint.to_string();
                    let pool_str = pool_pubkey.to_string();
                    let creator_str = creator.to_string();

                    if host
                        .pool_creator_cache_insert_if_absent(pool_str.clone(), creator_str.clone())
                    {
                        debug!(
                            pool = %pool_str,
                            creator = %creator_str,
                            "Cached creator from PumpAmm pool account (pool_creator_cache)"
                        );
                    }

                    if host.creator_cache_insert_if_absent(mint_str.clone(), creator_str.clone()) {
                        info!(
                            mint = %mint_str,
                            pool = %pool_str,
                            creator = %creator_str,
                            "Cached creator from PumpAmm pool account (migrated token)"
                        );
                    }
                }
                (
                    s.base_mint,
                    s.quote_mint,
                    s.base_reserve.unwrap_or(0),
                    s.quote_reserve.unwrap_or(0),
                )
            }
            CachedPoolState::PumpFun(s) => (
                s.token_mint,
                Pubkey::default(),
                s.virtual_token_reserves,
                s.virtual_sol_reserves,
            ),
            CachedPoolState::MeteoraCpmm(s) => {
                let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
                if s.token_1_mint == sol {
                    (s.token_0_mint, s.token_1_mint, s.reserve_0, s.reserve_1)
                } else if s.token_0_mint == sol {
                    (s.token_1_mint, s.token_0_mint, s.reserve_1, s.reserve_0)
                } else {
                    (s.token_0_mint, s.token_1_mint, s.reserve_0, s.reserve_1)
                }
            }
        };

        // Publish PoolCacheUpdate to JetStream (Single Source of Truth for pool state)
        if host.nats_enabled() {
            let mut pool_update = PoolCacheUpdate::new_pool_discovered(
                "market-data",
                host.build_version(),
                run_id,
                pool_pubkey.to_string(),
                cached_state.dex_name().to_string(),
                base_mint.to_string(),
                quote_mint.to_string(),
                base_reserve,
                quote_reserve,
                Some(0), // liquidity_lamports not available from account data
                *slot,
            );

            // Propagate DEX-specific metadata to SLAVE caches via PoolCacheUpdate.metadata.
            // This ensures execution-engine receives creator, pool accounts, etc. from Geyser
            // without needing RPC fallbacks.
            match &cached_state {
                CachedPoolState::PumpFun(s) => {
                    // SLAVE minimal state uses quote_mint for SOL side; must not be default.
                    pool_update.quote_mint = NATIVE_SOL_MINT.to_string();
                    // Always propagate real_reserves + complete for SELL validation
                    // in execution-engine's SLAVE cache.
                    let mut meta = std::collections::HashMap::new();
                    if s.creator != Pubkey::default() {
                        meta.insert("creator".to_string(), s.creator.to_string());
                    }
                    meta.insert(
                        "associated_bonding_curve".to_string(),
                        s.associated_bonding_curve.to_string(),
                    );
                    meta.insert("complete".to_string(), s.complete.to_string());
                    meta.insert(
                        "real_token_reserves".to_string(),
                        s.real_token_reserves.to_string(),
                    );
                    meta.insert(
                        "real_sol_reserves".to_string(),
                        s.real_sol_reserves.to_string(),
                    );
                    meta.insert(
                        "cashback_enabled".to_string(),
                        s.cashback_enabled.to_string(),
                    );
                    pool_update.metadata = Some(meta);
                    // Geyser-fed bonding curve: observation / partial only — not cold-path verified Ready.
                    pool_update.set_dex_readiness_in_metadata(DexPoolReadiness::Partial);
                    host.live_pool_cache()
                        .merge_pumpfun_bonding_readiness(*pool_pubkey, DexPoolReadiness::Partial);

                    // === BondingCurveProgress event for momentum-bot exit signal ===
                    // PumpFun initial real_token_reserves = 793_100_000_000_000
                    const INITIAL_REAL_TOKEN_RESERVES: u64 = 793_100_000_000_000;
                    let tokens_sold =
                        INITIAL_REAL_TOKEN_RESERVES.saturating_sub(s.real_token_reserves);
                    let progress_bps = ((tokens_sold as u128 * 10_000)
                        / INITIAL_REAL_TOKEN_RESERVES as u128)
                        as u32;
                    let progress_bps = progress_bps.min(10_000);

                    // Throttle: only emit when progress changes by >= 50 bps or complete changes
                    let should_emit =
                        { host.should_emit_curve_progress(pool_pubkey, progress_bps, s.complete) };

                    if should_emit {
                        host.record_curve_progress_emitted(*pool_pubkey, progress_bps, s.complete);

                        let curve_event = MarketEvent::new(
                            "market-data",
                            host.build_version(),
                            run_id,
                            host.next_event_id(),
                            "geyser_bonding_curve",
                            Some(*slot),
                            MarketEventKind::BondingCurveProgress {
                                mint: s.token_mint.to_string(),
                                bonding_curve: pool_pubkey.to_string(),
                                progress_bps,
                                complete: s.complete,
                            },
                        );

                        host.write_market_event_jsonl(&curve_event);

                        host.enqueue_core_market_event(
                            curve_event,
                            Some(MarketEventCorePublishTrace {
                                recv_at: *grpc_recv_at,
                                cold_path: false,
                                segment: MarketDataLatencySegment::BondingCurve,
                            }),
                        );
                    }
                }
                CachedPoolState::PumpAmm(s) => {
                    let mut meta = std::collections::HashMap::new();
                    if let Some(creator) = s.creator {
                        meta.insert("creator".to_string(), creator.to_string());
                    }
                    // FIX-26: pool_accounts from Geyser parse, or fallback to MASTER cache
                    let effective_pool_accounts = if !s.pool_accounts.is_empty() {
                        s.pool_accounts.clone()
                    } else {
                        host.live_pool_cache()
                            .get_pump_amm_pool_accounts(pool_pubkey)
                            .unwrap_or_default()
                    };
                    if !effective_pool_accounts.is_empty() {
                        let accounts_str: Vec<String> = effective_pool_accounts
                            .iter()
                            .map(|p| p.to_string())
                            .collect();
                        meta.insert("pool_accounts".to_string(), accounts_str.join(","));
                    }
                    let (ext_flag, ext_third, ext_t0, ext_t1) = host
                        .live_pool_cache()
                        .pump_amm_sell_extended_layout(pool_pubkey);
                    if ext_flag {
                        meta.insert(
                            "pump_amm_sell_cashback_remaining".to_string(),
                            "true".to_string(),
                        );
                    }
                    if let Some(pk) = ext_third.filter(|p| *p != Pubkey::default()) {
                        meta.insert(
                            "pump_amm_sell_cashback_third_meta".to_string(),
                            pk.to_string(),
                        );
                    }
                    if let Some(pk) = ext_t0.filter(|p| *p != Pubkey::default()) {
                        meta.insert("pump_amm_sell_extended_tail_0".to_string(), pk.to_string());
                    }
                    if let Some(pk) = ext_t1.filter(|p| *p != Pubkey::default()) {
                        meta.insert("pump_amm_sell_extended_tail_1".to_string(), pk.to_string());
                    }
                    if !meta.is_empty() {
                        pool_update.metadata = Some(meta);
                    }
                    // Geyser-fed accounts + reserves: stronger than observation-only, not Ready until verified trade/discovery.
                    pool_update.set_dex_readiness_in_metadata(DexPoolReadiness::Partial);
                    host.live_pool_cache()
                        .merge_pump_amm_pool_accounts_readiness(
                            *pool_pubkey,
                            DexPoolReadiness::Partial,
                        );
                }
                CachedPoolState::RaydiumAmm(s) => {
                    // FIX-29: Always propagate market_id (from Geyser parse),
                    // plus serum accounts when available (from async RPC fetch)
                    let mut meta = std::collections::HashMap::new();
                    if s.market_id != Pubkey::default() {
                        meta.insert("market_id".to_string(), s.market_id.to_string());
                    }
                    if let (Some(bids), Some(asks), Some(eq)) =
                        (s.serum_bids, s.serum_asks, s.serum_event_queue)
                    {
                        meta.insert("serum_bids".to_string(), bids.to_string());
                        meta.insert("serum_asks".to_string(), asks.to_string());
                        meta.insert("serum_event_queue".to_string(), eq.to_string());
                    }
                    if !meta.is_empty() {
                        pool_update.metadata = Some(meta);
                    }
                    let readiness = raydium_amm_readiness_for_pool_cache_update(s);
                    pool_update.set_dex_readiness_in_metadata(readiness);
                    host.live_pool_cache()
                        .merge_raydium_amm_pool_readiness(*pool_pubkey, readiness);
                }
                CachedPoolState::RaydiumCpmm(s) => {
                    let mut meta = std::collections::HashMap::new();
                    meta.insert(
                        POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY.to_string(),
                        raydium_cpmm_vaults_for_pool_cache_update(s),
                    );
                    pool_update.metadata = Some(meta);
                    let readiness = raydium_cpmm_readiness_for_pool_cache_update(s);
                    pool_update.set_dex_readiness_in_metadata(readiness);
                    host.live_pool_cache()
                        .merge_raydium_cpmm_pool_readiness(*pool_pubkey, readiness);
                }
                CachedPoolState::MeteoraCpmm(s) => {
                    let mut meta = std::collections::HashMap::new();
                    meta.insert(
                        POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY.to_string(),
                        meteora_cpmm_vaults_for_pool_cache_update(s),
                    );
                    meta.insert(
                        POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY.to_string(),
                        meteora_cpmm_onchain_mints_for_pool_cache_update(s),
                    );
                    pool_update.metadata = Some(meta);
                    let readiness = meteora_cpmm_readiness_for_pool_cache_update(s);
                    pool_update.set_dex_readiness_in_metadata(readiness);
                    host.live_pool_cache()
                        .merge_meteora_cpmm_pool_readiness(*pool_pubkey, readiness);
                }
                CachedPoolState::Orca(s) => {
                    pool_update.metadata = Some(orca_metadata_for_pool_cache_update(s));
                    let readiness = orca_readiness_for_pool_cache_update(s);
                    pool_update.set_dex_readiness_in_metadata(readiness);
                    host.live_pool_cache()
                        .merge_orca_pool_readiness(*pool_pubkey, readiness);
                }
                CachedPoolState::Meteora(s) => {
                    pool_update.metadata = Some(meteora_dlmm_metadata_for_pool_cache_update(s));
                    let readiness = meteora_dlmm_readiness_for_pool_cache_update(s);
                    pool_update.set_dex_readiness_in_metadata(readiness);
                    host.live_pool_cache()
                        .merge_meteora_dlmm_pool_readiness(*pool_pubkey, readiness);
                }
            }
            // P3 #13: Propagate base_decimals and quote_decimals to SLAVE caches (all DEX types)
            {
                let mut meta = pool_update.metadata.as_ref().cloned().unwrap_or_default();
                if let Some(d) = host.live_pool_cache().get_mint_decimals(&base_mint) {
                    meta.insert("base_decimals".to_string(), d.to_string());
                }
                // For quote: use quote_mint, or when default (PumpFun) use SOL
                let quote_for_decimals = if quote_mint == Pubkey::default() {
                    Pubkey::from_str(NATIVE_SOL_MINT).ok()
                } else {
                    Some(quote_mint)
                };
                if let Some(q) = quote_for_decimals {
                    if let Some(d) = host.live_pool_cache().get_mint_decimals(&q) {
                        meta.insert("quote_decimals".to_string(), d.to_string());
                    } else if q == Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default() {
                        meta.insert("quote_decimals".to_string(), "9".to_string());
                    }
                }
                if !meta.is_empty() {
                    pool_update.metadata = Some(meta);
                }
            }
            let subject = pool_subject(&pool_pubkey.to_string());
            sidefx_host_enqueue_jetstream(
                host,
                subject,
                &pool_update,
                "PoolCacheUpdate::new_pool_discovered",
                false,
            );
        }
    }
}

pub fn md_sidefx_process_vault_balance_tick(
    host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
    scratch: &mut MdSidefxBurstScratch,
) {
    let MdSidefxCommand::VaultBalanceTick {
        run_id,
        vault_pubkey,
        balance,
        slot,
        grpc_recv_at,
    } = job
    else {
        return;
    };
    let Some(vault_view) = host.vault_membership_view(vault_pubkey) else {
        return;
    };
    let prev_balance = vault_view
        .last_balance
        .swap(*balance, std::sync::atomic::Ordering::Relaxed);
    if *balance == prev_balance {
        inc_market_data_pool_state_publish_skipped_balance_unchanged_total();
        return;
    }
    scratch.note_vault_touch(*vault_pubkey);

    let (mut final_base, mut final_quote) = host
        .snapshot_vault_pair_balances(vault_pubkey, *balance)
        .unwrap_or((*balance, 0));

    // Prefer LivePoolCache MASTER when snapshot pair is incomplete.
    if final_base == 0 || final_quote == 0 {
        if let Some(state) = host.live_pool_cache().get(&vault_view.pool_address) {
            if let Some((_, _, cache_base, cache_quote, _)) =
                pool_cache_balance_fields_from_state(&state)
            {
                if cache_base > 0 && cache_quote > 0 {
                    final_base = cache_base;
                    final_quote = cache_quote;
                }
            }
        }
    }

    if final_base == 0 && final_quote == 0 {
        return;
    }

    host.live_pool_cache()
        .update_vault_balance(vault_pubkey, *balance, *slot);

    if host.nats_enabled() {
        let mut balance_update = PoolCacheUpdate::new_balance_updated(
            "market-data",
            host.build_version(),
            run_id,
            vault_view.pool_address.to_string(),
            vault_view.dex.clone(),
            vault_view.base_mint.to_string(),
            vault_view.quote_mint.to_string(),
            final_base,
            final_quote,
            *slot,
        );
        if vault_view.dex == "raydium_cpmm" {
            if let Some(CachedPoolState::RaydiumCpmm(ref s)) =
                host.live_pool_cache().get(&vault_view.pool_address)
            {
                let mut meta = std::collections::HashMap::new();
                meta.insert(
                    POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY.to_string(),
                    raydium_cpmm_vaults_for_pool_cache_update(s),
                );
                balance_update.metadata = Some(meta);
                let readiness = raydium_cpmm_readiness_for_pool_cache_update(s);
                balance_update.set_dex_readiness_in_metadata(readiness);
                host.live_pool_cache()
                    .merge_raydium_cpmm_pool_readiness(vault_view.pool_address, readiness);
            }
        }
        if vault_view.dex == "meteora_cpmm" {
            if let Some(CachedPoolState::MeteoraCpmm(ref s)) =
                host.live_pool_cache().get(&vault_view.pool_address)
            {
                let mut meta = balance_update.metadata.take().unwrap_or_default();
                meta.insert(
                    POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY.to_string(),
                    meteora_cpmm_vaults_for_pool_cache_update(s),
                );
                meta.insert(
                    POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY.to_string(),
                    meteora_cpmm_onchain_mints_for_pool_cache_update(s),
                );
                balance_update.metadata = Some(meta);
                let readiness = meteora_cpmm_readiness_for_pool_cache_update(s);
                balance_update.set_dex_readiness_in_metadata(readiness);
                host.live_pool_cache()
                    .merge_meteora_cpmm_pool_readiness(vault_view.pool_address, readiness);
            }
        }
        if vault_view.dex == "raydium" {
            if let Some(CachedPoolState::RaydiumAmm(ref s)) =
                host.live_pool_cache().get(&vault_view.pool_address)
            {
                let readiness = raydium_amm_readiness_for_pool_cache_update(s);
                balance_update.set_dex_readiness_in_metadata(readiness);
                host.live_pool_cache()
                    .merge_raydium_amm_pool_readiness(vault_view.pool_address, readiness);
            }
        }
        if vault_view.dex == "orca" {
            if let Some(CachedPoolState::Orca(ref s)) =
                host.live_pool_cache().get(&vault_view.pool_address)
            {
                let mut meta = balance_update.metadata.take().unwrap_or_default();
                for (k, v) in orca_metadata_for_pool_cache_update(s) {
                    meta.insert(k, v);
                }
                balance_update.metadata = Some(meta);
                let readiness = orca_readiness_for_pool_cache_update(s);
                balance_update.set_dex_readiness_in_metadata(readiness);
                host.live_pool_cache()
                    .merge_orca_pool_readiness(vault_view.pool_address, readiness);
            }
        }
        if vault_view.dex == "meteora_dlmm" {
            if let Some(CachedPoolState::Meteora(ref s)) =
                host.live_pool_cache().get(&vault_view.pool_address)
            {
                let mut meta = balance_update.metadata.take().unwrap_or_default();
                for (k, v) in meteora_dlmm_metadata_for_pool_cache_update(s) {
                    meta.insert(k, v);
                }
                balance_update.metadata = Some(meta);
                let readiness = meteora_dlmm_readiness_for_pool_cache_update(s);
                balance_update.set_dex_readiness_in_metadata(readiness);
                host.live_pool_cache()
                    .merge_meteora_dlmm_pool_readiness(vault_view.pool_address, readiness);
            }
        }
        let subject = pool_subject(&vault_view.pool_address.to_string());
        sidefx_host_enqueue_jetstream(
            host,
            subject,
            &balance_update,
            "PoolCacheUpdate::BalanceUpdated",
            false,
        );
        info!(
            pool = %vault_view.pool_address,
            slot = slot,
            "MASTER CACHE: PoolCacheUpdate::BalanceUpdated enqueued for JetStream"
        );
    }

    let state_event = MarketEvent::new(
        "market-data",
        host.build_version(),
        run_id,
        host.next_event_id(),
        "geyser_vault",
        Some(*slot),
        MarketEventKind::PoolStateUpdate {
            pool_address: vault_view.pool_address.to_string(),
            dex: vault_view.dex.clone(),
            reserve_base: final_base,
            reserve_quote: final_quote,
            base_mint: vault_view.base_mint.to_string(),
            quote_mint: vault_view.quote_mint.to_string(),
            update_slot: *slot,
            active_id: vault_view.active_id,
            bin_step: vault_view.bin_step,
        },
    );

    host.write_market_event_jsonl(&state_event);

    if host.nats_enabled() {
        host.enqueue_core_market_event(
            state_event,
            Some(MarketEventCorePublishTrace {
                recv_at: *grpc_recv_at,
                cold_path: false,
                segment: MarketDataLatencySegment::Other,
            }),
        );
    }
}

pub fn md_sidefx_process_touch_bin_array_tick(
    _host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
    scratch: &mut MdSidefxBurstScratch,
) {
    let MdSidefxCommand::TouchBinArrayTick { pda } = job else {
        return;
    };
    scratch.note_bin_array_touch(*pda);
}

pub fn md_sidefx_process_trade_pool_lru_touch(
    host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
    scratch: &mut MdSidefxBurstScratch,
) {
    let MdSidefxCommand::TradePoolLruTouch { pool } = job else {
        return;
    };
    host.note_trade_pool_lru_touches(*pool, scratch);
}

pub fn md_sidefx_process_job(
    host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
    scratch: &mut MdSidefxBurstScratch,
) {
    match job {
        MdSidefxCommand::PumpFunPoolMintMapInsert { .. } => {
            md_sidefx_process_pump_fun_pool_mint_map(host, job)
        }
        MdSidefxCommand::PumpFunDevWalletFromPoolCreated { .. } => {
            md_sidefx_process_pump_fun_dev_wallet_from_pool_created(host, job)
        }
        MdSidefxCommand::PumpAmmCreatePoolObserved { .. } => {
            md_sidefx_process_pump_amm_create_pool(host, job)
        }
        MdSidefxCommand::PumpAmmTradeWithAccounts { .. } => {
            md_sidefx_process_pump_amm_trade(host, job)
        }
        MdSidefxCommand::GenericDexFirstTradeAccounts { .. } => {
            md_sidefx_process_generic_dex_first_trade(host, job)
        }
        MdSidefxCommand::BondingCurveDevWallet { .. } => md_sidefx_process_bonding_curve(host, job),
        MdSidefxCommand::VaultBalanceTick { .. } => {
            md_sidefx_process_vault_balance_tick(host, job, scratch)
        }
        MdSidefxCommand::TouchBinArrayTick { .. } => {
            md_sidefx_process_touch_bin_array_tick(host, job, scratch)
        }
        MdSidefxCommand::DlmmPoolStatePublishSignal { .. } => {
            md_sidefx_process_dlmm_pool_state_publish_signal(host, job, scratch)
        }
        MdSidefxCommand::TradePoolLruTouch { .. } => {
            md_sidefx_process_trade_pool_lru_touch(host, job, scratch)
        }
        MdSidefxCommand::LivePoolCacheAccountUpdate { .. } => {
            md_sidefx_process_live_pool_cache_account_update(host, job, scratch)
        }
        MdSidefxCommand::LivePoolCacheMintDecimals { .. } => {
            md_sidefx_process_live_pool_cache_mint_decimals(host, job)
        }
    }
}
