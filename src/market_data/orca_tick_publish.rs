//! Orca Whirlpool TickArray Geyser publish (Job 2 ingest).

use crate::ipc::{MarketEvent, MarketEventKind, OrcaTickSnapshot};
use crate::market_data::ingest::account_host::{AccountIngestHost, AccountOrcaTickArrayView};
use crate::market_data::ingest::account_parse::account_publish_segment;
use crate::market_data::ingest::AccountUpdateClass;
use crate::market_data::publish::{account_path_enqueue_core_market_event, AccountPublishSender};
use crate::market_data::sidefx::{
    md_account_sidefx_try_enqueue_classed, MarketEventCorePublishTrace, MdAccountSidefxSender,
    MdSidefxCommand, SidefxUpdateClass,
};
use crate::metrics::{
    inc_market_data_orca_tick_array_membership_hit_total,
    inc_market_data_orca_tick_array_parse_fail_total,
    inc_market_data_orca_tick_array_publish_total,
};
use crate::solana::dex::orca_tick_array::parse_tick_array;
use crate::solana::geyser_listener::GeyserAccountUpdate;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrcaTickArrayPublishOutcome {
    Published,
    ParseFailed,
    SkippedNoNats,
}

fn parsed_ticks_to_snapshots(
    parsed: &crate::solana::dex::orca_tick_array::ParsedTickArray,
) -> Vec<OrcaTickSnapshot> {
    parsed
        .ticks
        .iter()
        .map(|t| OrcaTickSnapshot {
            tick_index: t.tick_index,
            initialized: t.initialized,
            liquidity_net: t.liquidity_net.to_string(),
        })
        .collect()
}

/// Parse + publish one Orca Whirlpool tick-array Geyser update for a tracked membership row.
#[allow(clippy::too_many_arguments)]
pub async fn publish_orca_tick_array_from_geyser<H: AccountIngestHost>(
    host: &H,
    run_id: &str,
    account_geyser_recv_at: Instant,
    account_update: &GeyserAccountUpdate,
    tick_info: AccountOrcaTickArrayView,
    publish_tx: Option<&AccountPublishSender>,
    md_sidefx: Option<&MdAccountSidefxSender>,
    sidefx_class: SidefxUpdateClass,
    update_class: AccountUpdateClass,
) -> OrcaTickArrayPublishOutcome {
    inc_market_data_orca_tick_array_membership_hit_total();
    if let Some(md_sidefx) = md_sidefx {
        md_account_sidefx_try_enqueue_classed(
            md_sidefx,
            sidefx_class,
            MdSidefxCommand::TouchBinArrayTick {
                pda: account_update.pubkey,
                update_class: sidefx_class,
            },
        );
    }
    match parse_tick_array(&account_update.data) {
        Some(parsed) => {
            let ticks = parsed_ticks_to_snapshots(&parsed);
            let tick_event = MarketEvent::new(
                "market-data",
                host.account_build_version(),
                run_id,
                host.account_next_event_id(),
                "geyser_orca_tick_array",
                Some(account_update.slot),
                MarketEventKind::OrcaTickArrayUpdate {
                    pool_address: tick_info.pool_address.to_string(),
                    start_tick_index: tick_info.start_tick_index,
                    ticks,
                    update_slot: account_update.slot,
                },
            );
            host.account_write_market_event_jsonl(&tick_event);
            if host.account_nats().is_some() {
                let _ = update_class;
                inc_market_data_orca_tick_array_publish_total();
                let seg = account_publish_segment(&tick_event.kind);
                account_path_enqueue_core_market_event(
                    publish_tx,
                    host.account_nats(),
                    host.account_publish_host(),
                    tick_event,
                    Some(MarketEventCorePublishTrace {
                        recv_at: account_geyser_recv_at,
                        cold_path: false,
                        segment: seg,
                    }),
                )
                .await;
                return OrcaTickArrayPublishOutcome::Published;
            }
            OrcaTickArrayPublishOutcome::SkippedNoNats
        }
        None => {
            inc_market_data_orca_tick_array_parse_fail_total();
            OrcaTickArrayPublishOutcome::ParseFailed
        }
    }
}
