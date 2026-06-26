//! Phase 5a: track-worker Geyser explicit-set rebuild + coalesced delta-only push.

use super::desired_set::{symmetric_diff, ConsumerId, DesiredExplicitSet};
use super::worker::TrackWorkerContext;
use crate::metrics::{
    inc_market_data_geyser_sync_skipped_no_delta_total,
    record_market_data_geyser_subscribe_delta_pubkeys,
    record_market_data_md_state_sync_flush_duration_us, set_market_data_geyser_explicit_set_size,
    set_market_data_geyser_sync_pending,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// PR235: wall time budget for sync/evict slice after job batch (track-worker only).
pub const MARKET_DATA_MD_STATE_FLUSH_BUDGET_MS: u64 = 8;

pub fn explicit_subscription_has_new_keys(
    before: &HashSet<Pubkey>,
    after: &HashSet<Pubkey>,
) -> bool {
    after.iter().any(|k| !before.contains(k))
}

pub fn rebuild_desired_explicit_set_from_ctx<C: TrackWorkerContext>(
    ctx: &C,
    set: &mut DesiredExplicitSet,
) {
    set.clear();
    set.set_max_explicit_pubkeys(ctx.max_tracked_accounts());
    for (pk, consumer, pool) in ctx.explicit_pubkey_rows_for_desired_set() {
        set.insert(pk, consumer, pool);
    }
    set_market_data_geyser_explicit_set_size(set.len());
}

pub fn track_worker_execute_coalesced_push<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    desired: &mut DesiredExplicitSet,
    before_keys: HashSet<Pubkey>,
    continue_evict: bool,
    release_flush_slot: bool,
) -> bool {
    rebuild_desired_explicit_set_from_ctx(ctx.as_ref(), desired);
    let after_mutations = ctx.snapshot_explicit_subscription_pubkeys();
    let delta = symmetric_diff(&before_keys, &after_mutations);
    if delta.is_empty() && !continue_evict && !ctx.pending_geyser_evict() {
        inc_market_data_geyser_sync_skipped_no_delta_total();
        set_market_data_geyser_explicit_set_size(desired.len());
        set_market_data_geyser_sync_pending(0);
        if release_flush_slot {
            ctx.release_geyser_sync_flush_slot();
        }
        return true;
    }
    if !delta.is_empty() {
        record_market_data_geyser_subscribe_delta_pubkeys(delta.len() as u64);
    }
    let flush_deadline =
        Instant::now() + Duration::from_millis(MARKET_DATA_MD_STATE_FLUSH_BUDGET_MS);
    let flush_start = Instant::now();
    let sync_complete = if continue_evict {
        ctx.continue_geyser_evict_with_deadline(flush_deadline)
    } else {
        ctx.sync_geyser_tracked_accounts_batched_flush_with_deadline(flush_deadline)
    };
    set_market_data_geyser_sync_pending(0);
    if release_flush_slot {
        ctx.release_geyser_sync_flush_slot();
    }
    record_market_data_md_state_sync_flush_duration_us(
        flush_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
    );
    rebuild_desired_explicit_set_from_ctx(ctx.as_ref(), desired);
    ctx.refresh_tracked_membership_snapshot();
    if !sync_complete {
        return false;
    }
    true
}

pub fn consumer_id_for_track_pin(pin: super::worker::TrackPinReason) -> ConsumerId {
    match pin {
        super::worker::TrackPinReason::Wallet => ConsumerId::Wallet,
        super::worker::TrackPinReason::MomentumActive => ConsumerId::Momentum,
        super::worker::TrackPinReason::ArbMultiDex => ConsumerId::Arb,
    }
}
