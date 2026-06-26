//! Core NATS + momentum fan-out publish helpers.

use super::host::PublishHost;
use crate::ipc::{MarketEvent, MarketEventKind, NATIVE_SOL_MINT};
use crate::market_data::sidefx::host::{
    market_event_should_nats_core, MarketEventCorePublishTrace,
};
use crate::metrics::{
    record_market_data_bonding_to_trade_slot_delta_slots,
    record_market_data_geyser_to_publish_on_success,
    record_market_data_trade_after_bonding_publish_ms, wall_clock_unix_ms_now,
    MARKET_DATA_LAST_TRADE_PUBLISH_TS_UNIX_MS, MARKET_EVENTS_MOMENTUM_FANOUT_PUBLISHED_TOTAL,
    MARKET_EVENTS_PUBLISHED_TOTAL, NATS_ERRORS_TOTAL, NATS_MESSAGES_PUBLISHED_TOTAL,
};
use crate::nats::{NatsClient, TOPIC_MARKET_EVENTS, TOPIC_MOMENTUM_MARKET_EVENTS};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use tracing::warn;

/// Subset of [`MarketEventKind`] mirrored to [`TOPIC_MOMENTUM_MARKET_EVENTS`] in addition to core.
pub fn market_event_is_momentum_nats_relevant(kind: &MarketEventKind) -> bool {
    match kind {
        MarketEventKind::Trade { .. }
        | MarketEventKind::PoolCreated { .. }
        | MarketEventKind::BondingCurveProgress { .. }
        | MarketEventKind::TokenMintInfo { .. }
        | MarketEventKind::DexPoolAccounts { .. }
        | MarketEventKind::DevWalletIdentified { .. }
        | MarketEventKind::LiquidityRemoved { .. }
        | MarketEventKind::WalletSnapshotComplete { .. } => true,

        MarketEventKind::PoolStateUpdate {
            dex, quote_mint, ..
        } => {
            let dex_lc = dex.to_ascii_lowercase();
            let pump_line = matches!(
                dex_lc.as_str(),
                "pumpfun" | "pump_amm" | "pumpfunamm" | "pumpfun_amm"
            );
            if !pump_line {
                return false;
            }
            let q = quote_mint.as_str();
            q == NATIVE_SOL_MINT
        }

        MarketEventKind::LatestBlockhash { .. }
        | MarketEventKind::BinArrayUpdate { .. }
        | MarketEventKind::SlotUpdate { .. }
        | MarketEventKind::WalletBalanceSnapshot { .. }
        | MarketEventKind::PriceUpdate { .. }
        | MarketEventKind::SwapObserved { .. }
        | MarketEventKind::AccountUpdate { .. }
        | MarketEventKind::TransactionDetected { .. }
        | MarketEventKind::WalletActivity { .. }
        | MarketEventKind::EarlyBuyerDetected { .. }
        | MarketEventKind::InsiderAlert { .. }
        | MarketEventKind::WalletTxConfirmed { .. } => false,
    }
}

/// Publish one logical market event to core NATS and, when momentum-relevant, to momentum subject.
pub async fn publish_market_event_core_and_momentum_ex(
    nats: &NatsClient,
    event: &MarketEvent,
    trace: Option<MarketEventCorePublishTrace>,
    host: Option<&dyn PublishHost>,
) -> bool {
    if !market_event_should_nats_core(&event.kind) {
        return false;
    }
    match nats.publish(TOPIC_MARKET_EVENTS, event).await {
        Ok(true) => {
            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
            if let Some(t) = trace {
                record_market_data_geyser_to_publish_on_success(
                    t.recv_at,
                    t.segment,
                    t.cold_path,
                    event.slot,
                );
            }
            if let Some(host) = host {
                let now_ms = wall_clock_unix_ms_now();
                match &event.kind {
                    MarketEventKind::Trade {
                        pool_address, dex, ..
                    } if dex == "pumpfun" => {
                        host.on_pumpfun_trade_core_published(
                            pool_address,
                            now_ms,
                            event.slot.filter(|&s| s > 0),
                        );
                    }
                    MarketEventKind::BondingCurveProgress { bonding_curve, .. } => {
                        host.on_bonding_curve_progress_core_published(
                            bonding_curve,
                            now_ms,
                            event.slot,
                        );
                    }
                    _ => {}
                }
            }
        }
        Ok(false) => {
            warn!(
                event_id = %event.event_id,
                topic = TOPIC_MARKET_EVENTS,
                "Market event publish to NATS (core) dropped or failed"
            );
            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        Err(e) => {
            warn!(
                error = %e,
                event_id = %event.event_id,
                "Failed to publish market event to NATS (core)"
            );
            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return false;
        }
    }

    if market_event_is_momentum_nats_relevant(&event.kind) {
        match nats.publish(TOPIC_MOMENTUM_MARKET_EVENTS, event).await {
            Ok(true) => {
                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                MARKET_EVENTS_MOMENTUM_FANOUT_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => {
                warn!(
                    event_id = %event.event_id,
                    topic = TOPIC_MOMENTUM_MARKET_EVENTS,
                    "Market event publish to NATS (momentum fan-out) dropped or failed"
                );
                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                warn!(
                    error = %e,
                    event_id = %event.event_id,
                    topic = TOPIC_MOMENTUM_MARKET_EVENTS,
                    "Failed to publish market event to NATS (momentum fan-out)"
                );
                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    true
}

/// Default pumpfun trade side effects using [`PublishHost`] bonding-curve lookups.
pub fn publish_host_on_pumpfun_trade(
    host: &dyn PublishHost,
    pool_address: &str,
    now_ms: u64,
    trade_slot: Option<u64>,
) {
    MARKET_DATA_LAST_TRADE_PUBLISH_TS_UNIX_MS.store(now_ms, Ordering::Relaxed);
    if let Ok(pk) = Pubkey::from_str(pool_address) {
        if let Some(last) = host.last_bonding_wall_ms(&pk) {
            if now_ms >= last {
                record_market_data_trade_after_bonding_publish_ms(now_ms.saturating_sub(last));
            }
        }
        if let (Some(bond_slot), Some(trade_slot)) = (host.last_bonding_slot(&pk), trade_slot) {
            let delta = trade_slot.saturating_sub(bond_slot);
            record_market_data_bonding_to_trade_slot_delta_slots(delta);
        }
    }
}
