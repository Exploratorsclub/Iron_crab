//! Geyser transaction ingest handler (dedizierte Task-Fairness, MARKET-DATA-TX-INGEST-FAIRNESS).
//! STOP-CHECK: keine neuen RPC-Calls.

use super::tx_host::TxIngestHost;
use super::tx_parse::{
    maybe_emit_dev_wallet_after_pool_mint_map, process_wallet_balance_snapshots_from_tx_meta,
    resolve_pumpfun_creator_tx_path, tx_publish_segment,
};
use crate::ipc::{IntentTier, MarketEvent, MarketEventKind, PriorityFeePercentiles};
use crate::market_data::md_state::{md_state_try_enqueue, MdStateCommand, MdStateSender};
use crate::market_data::publish::{
    account_path_enqueue_core_market_event, account_path_enqueue_priority_fee_sample,
    try_enqueue_account_path_nats_job, AccountPathNatsJob, AccountPublishSender,
};
use crate::market_data::sidefx::{
    host::market_event_should_nats_core, md_sidefx_try_enqueue, MarketEventCorePublishTrace,
    MdSidefxCommand, MdSidefxSender,
};
use crate::metrics::{
    inc_market_data_unparsed_tx_dropped_total, market_data_bump_geyser_head_slot,
    record_market_data_tx_channel_lag_ms, record_market_data_tx_handler_processed,
};
use crate::nats::TOPIC_PRIORITY_FEE_SAMPLES;
use crate::solana::dex_parser::{
    parse_transaction_update_with_pool_lookup, DexType, ParsedDexEvent,
};
use crate::solana::geyser_listener::GeyserTransactionUpdate;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, info};

/// Geyser transaction ingest (dedizierte Task-Fairness, siehe MARKET-DATA-TX-INGEST-FAIRNESS).
/// STOP-CHECK: keine neuen RPC-Calls; gleiche Logik wie zuvor im `select!`-Arm.
pub async fn handle_geyser_transaction_update<H: TxIngestHost>(
    host: &H,
    run_id: &str,
    tx_update: GeyserTransactionUpdate,
    tx_count: &AtomicU64,
    account_publish_tx: Option<&AccountPublishSender>,
    md_state: &MdStateSender,
    md_sidefx: &MdSidefxSender,
) {
    record_market_data_tx_handler_processed();
    let recv_at = Instant::now();
    record_market_data_tx_channel_lag_ms(tx_update.grpc_recv_at, recv_at);
    let tx_geyser_recv_at = recv_at;
    market_data_bump_geyser_head_slot(tx_update.slot);
    tx_count.fetch_add(1, Ordering::Relaxed);
    crate::metrics::record_activity();

    process_wallet_balance_snapshots_from_tx_meta(
        host,
        run_id,
        &tx_update,
        account_publish_tx,
        md_state,
    )
    .await;

    // P2: Track priority fees from Geyser transactions (NO RPC calls!)
    if let Some(priority_fee) = host.tx_priority_fee_add_sample(
        tx_update.slot,
        tx_update.fee_lamports,
        tx_update.compute_units_consumed,
    ) {
        let sample_count = host.tx_priority_fee_sample_count();
        if sample_count % 50 == 0 && sample_count >= 10 {
            let percentiles = host.tx_priority_fee_percentiles();
            let fee_msg = PriorityFeePercentiles::new(
                "market-data",
                host.tx_build_version(),
                host.tx_run_id(),
                percentiles.sample_count,
                percentiles.last_slot,
                percentiles.p25,
                percentiles.p50,
                percentiles.p75,
                percentiles.p90,
                host.tx_priority_fee_for_tier(IntentTier::Tier0),
                host.tx_priority_fee_for_tier(IntentTier::Tier1),
                host.tx_priority_fee_for_tier(IntentTier::Arb),
            );
            if let Some(publish_tx) = account_publish_tx {
                account_path_enqueue_priority_fee_sample(
                    Some(publish_tx),
                    host.tx_nats(),
                    &fee_msg,
                )
                .await;
            } else if let Some(nats) = host.tx_nats() {
                if let Err(e) = nats.publish(TOPIC_PRIORITY_FEE_SAMPLES, &fee_msg).await {
                    debug!(error = %e, "Failed to publish priority fee percentiles");
                }
            }
            info!(
                samples = sample_count,
                p50 = percentiles.p50,
                p90 = percentiles.p90,
                tier1_recommended = host.tx_priority_fee_for_tier(IntentTier::Tier1),
                last_fee = priority_fee,
                "priority_fee: published percentiles"
            );
        }
    }

    let pool_lookup = |pool: &Pubkey| host.tx_orca_pool_lookup(pool);
    let parsed_event = parse_transaction_update_with_pool_lookup(&tx_update, Some(&pool_lookup));

    if let Some(parsed) = parsed_event.as_ref() {
        let mint_and_dex: Option<(Pubkey, Option<DexType>)> = match parsed {
            ParsedDexEvent::PoolCreated { base_mint, dex, .. } => Some((*base_mint, Some(*dex))),
            ParsedDexEvent::Trade { mint, dex, .. } => Some((*mint, Some(*dex))),
            ParsedDexEvent::LiquidityRemoved { mint, .. } => Some((*mint, None)),
            ParsedDexEvent::BondingCurveUpdate { .. } => None,
        };
        if let Some((mint, dex_opt)) = mint_and_dex {
            md_state_try_enqueue(md_state, MdStateCommand::TrackMint { mint, pin: None });
            debug!(
                mint = %mint,
                dex = ?dex_opt,
                "Mint track enqueued for Geyser metadata (batched sync via md-state), waiting for mint account delivery"
            );
        }

        match parsed {
            ParsedDexEvent::PoolCreated {
                pool_address,
                base_mint,
                creator,
                dex: DexType::PumpFun,
                ..
            } => {
                md_sidefx_try_enqueue(
                    md_sidefx,
                    MdSidefxCommand::PumpFunPoolMintMapInsert {
                        run_id: run_id.to_string(),
                        pool_address: *pool_address,
                        mint_str: base_mint.to_string(),
                        slot: Some(tx_update.slot),
                        tx_grpc_recv_at: tx_update.grpc_recv_at,
                        creator_override: *creator,
                    },
                );
            }
            ParsedDexEvent::Trade {
                pool_address,
                mint,
                creator,
                dex: DexType::PumpFun,
                ..
            } => {
                md_sidefx_try_enqueue(
                    md_sidefx,
                    MdSidefxCommand::PumpFunPoolMintMapInsert {
                        run_id: run_id.to_string(),
                        pool_address: *pool_address,
                        mint_str: mint.to_string(),
                        slot: Some(tx_update.slot),
                        tx_grpc_recv_at: tx_update.grpc_recv_at,
                        creator_override: *creator,
                    },
                );
            }
            _ => {}
        }
    }

    if let Some(ParsedDexEvent::Trade { pool_address, .. }) = parsed_event.as_ref() {
        md_sidefx_try_enqueue(
            md_sidefx,
            MdSidefxCommand::TradePoolLruTouch {
                pool: *pool_address,
            },
        );
    }

    let wallet_events = if let Some(ref parsed) = parsed_event {
        match parsed {
            ParsedDexEvent::PoolCreated { base_mint, .. } => {
                host.tx_record_pool_created(&base_mint.to_string(), tx_update.slot);
                Vec::new()
            }
            ParsedDexEvent::Trade {
                mint,
                trader,
                is_buy,
                sol_amount,
                token_amount,
                signature,
                slot,
                ..
            } => host.tx_wallet_tracker_process_trade(
                &mint.to_string(),
                &trader.to_string(),
                *is_buy,
                *sol_amount,
                *token_amount,
                *slot,
                signature,
            ),
            ParsedDexEvent::LiquidityRemoved { .. } => Vec::new(),
            ParsedDexEvent::BondingCurveUpdate { .. } => Vec::new(),
        }
    } else {
        Vec::new()
    };

    for wallet_event in wallet_events {
        host.tx_write_market_event_jsonl(&wallet_event);
        if !market_event_should_nats_core(&wallet_event.kind) {
            continue;
        }
        let Some(publish_tx) = account_publish_tx else {
            continue;
        };
        let seg = tx_publish_segment(&wallet_event.kind);
        let _ = try_enqueue_account_path_nats_job(
            publish_tx,
            AccountPathNatsJob::CoreMarketEvent {
                event: Box::new(wallet_event),
                trace: Some(MarketEventCorePublishTrace {
                    recv_at: tx_geyser_recv_at,
                    cold_path: false,
                    segment: seg,
                }),
            },
            "wallet tracking MarketEvent",
        );
    }

    if let Some(ParsedDexEvent::PoolCreated {
        pool_address,
        base_mint: base_mint_pk,
        quote_mint: quote_mint_pk,
        dex: DexType::PumpFunAmm,
        ..
    }) = parsed_event.as_ref()
    {
        md_sidefx_try_enqueue(
            md_sidefx,
            MdSidefxCommand::PumpAmmCreatePoolObserved {
                run_id: run_id.to_string(),
                pool_address: *pool_address,
                base_mint: base_mint_pk.to_string(),
                quote_mint: quote_mint_pk.to_string(),
                slot: tx_update.slot,
                tx_geyser_recv_at,
            },
        );
    }

    if let Some(ParsedDexEvent::Trade {
        pool_address,
        mint: base_mint_pk,
        dex: DexType::PumpFunAmm,
        is_buy,
        pool_accounts: Some(pool_accounts),
        pump_amm_sell_requires_cashback_remaining,
        pump_amm_sell_cashback_third_meta,
        pump_amm_sell_extended_tail_0,
        pump_amm_sell_extended_tail_1,
        pump_amm_sell_extended_fee_tail_0,
        pump_amm_sell_extended_fee_tail_1,
        pump_amm_sell_requires_fee_tail,
        pump_amm_sell_requires_pre_fee_metas,
        pump_amm_sell_pre_fee_meta_1,
        ..
    }) = parsed_event.as_ref()
    {
        md_sidefx_try_enqueue(
            md_sidefx,
            MdSidefxCommand::PumpAmmTradeWithAccounts {
                run_id: run_id.to_string(),
                pool_address: *pool_address,
                base_mint_pk: *base_mint_pk,
                slot: tx_update.slot,
                is_buy: *is_buy,
                pool_accounts: pool_accounts.clone(),
                pump_amm_sell_requires_cashback_remaining:
                    *pump_amm_sell_requires_cashback_remaining,
                pump_amm_sell_cashback_third_meta: *pump_amm_sell_cashback_third_meta,
                pump_amm_sell_extended_tail_0: *pump_amm_sell_extended_tail_0,
                pump_amm_sell_extended_tail_1: *pump_amm_sell_extended_tail_1,
                pump_amm_sell_extended_fee_tail_0: *pump_amm_sell_extended_fee_tail_0,
                pump_amm_sell_extended_fee_tail_1: *pump_amm_sell_extended_fee_tail_1,
                pump_amm_sell_requires_fee_tail: *pump_amm_sell_requires_fee_tail,
                pump_amm_sell_requires_pre_fee_metas: *pump_amm_sell_requires_pre_fee_metas,
                pump_amm_sell_pre_fee_meta_1: *pump_amm_sell_pre_fee_meta_1,
                tx_geyser_recv_at,
            },
        );
    }

    if let Some(ParsedDexEvent::Trade {
        pool_address,
        mint,
        quote_mint,
        dex,
        pool_accounts: Some(pool_accounts),
        ..
    }) = parsed_event.as_ref()
    {
        if !matches!(dex, DexType::PumpFunAmm) {
            md_sidefx_try_enqueue(
                md_sidefx,
                MdSidefxCommand::GenericDexFirstTradeAccounts {
                    run_id: run_id.to_string(),
                    pool_address: *pool_address,
                    mint: *mint,
                    quote_mint: *quote_mint,
                    dex: *dex,
                    pool_accounts: pool_accounts.clone(),
                    slot: tx_update.slot,
                    tx_geyser_recv_at,
                },
            );
        }
    }

    let Some(parsed) = parsed_event else {
        inc_market_data_unparsed_tx_dropped_total();
        return;
    };

    let pumpfun_trade_creator = match &parsed {
        ParsedDexEvent::Trade {
            dex: DexType::PumpFun,
            creator,
            ..
        } => creator.as_ref(),
        _ => None,
    };

    info!(
        slot = tx_update.slot,
        sig = %tx_update.signature,
        "Parsed DEX transaction"
    );
    let mut kind = parsed.to_market_event_kind();

    if let MarketEventKind::Trade {
        ref pool_address,
        ref mint,
        ref dex,
        ref mut creator,
        ..
    } = kind
    {
        if dex == "pumpfun" || dex == "pump_amm" {
            if let Ok(pool_pk) = Pubkey::from_str(pool_address) {
                if let Some(cached_creator) = resolve_pumpfun_creator_tx_path(
                    host,
                    pool_address,
                    mint,
                    &pool_pk,
                    pumpfun_trade_creator,
                ) {
                    host.tx_pool_creator_cache_insert(pool_address.clone(), cached_creator.clone());
                    host.tx_creator_cache_insert(mint.clone(), cached_creator.clone());
                    *creator = Some(cached_creator.clone());
                    let _ = maybe_emit_dev_wallet_after_pool_mint_map(
                        host,
                        run_id,
                        &pool_pk,
                        mint,
                        Some(tx_update.slot),
                        tx_geyser_recv_at,
                        account_publish_tx,
                        host.tx_nats(),
                        Some(cached_creator.as_str()),
                    )
                    .await;
                }
            }
        }
    }

    let event_kind = kind;

    let event = MarketEvent::new(
        "market-data",
        host.tx_build_version(),
        run_id,
        host.tx_next_event_id(),
        "geyser",
        Some(tx_update.slot),
        event_kind,
    );

    host.tx_write_market_event_jsonl(&event);

    if host.tx_nats().is_some() {
        let seg = tx_publish_segment(&event.kind);
        let _ = account_path_enqueue_core_market_event(
            account_publish_tx,
            host.tx_nats(),
            host.tx_publish_host(),
            event,
            Some(MarketEventCorePublishTrace {
                recv_at: tx_geyser_recv_at,
                cold_path: false,
                segment: seg,
            }),
        )
        .await;
    }
}
