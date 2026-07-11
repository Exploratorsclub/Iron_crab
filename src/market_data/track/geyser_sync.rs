//! Phase 5a: track-worker Geyser explicit-set rebuild + coalesced delta-only push.

use super::desired_set::{symmetric_diff, DesiredExplicitSet};
use super::worker::TrackWorkerContext;
use crate::metrics::{
    inc_market_data_geyser_sync_skipped_no_delta_total,
    record_market_data_geyser_subscribe_delta_pubkeys,
    record_market_data_md_state_sync_flush_duration_us,
    set_market_data_geyser_explicit_admitted_accounts,
    set_market_data_geyser_explicit_cap_overflow, set_market_data_geyser_explicit_set_size,
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
    ctx.converge_explicit_admission(set);
    set_market_data_geyser_explicit_set_size(set.len());
}

pub fn track_worker_execute_coalesced_push<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    desired: &mut DesiredExplicitSet,
    before_keys: HashSet<Pubkey>,
    continue_evict: bool,
    release_flush_slot: bool,
    admission_converged: &mut bool,
) -> bool {
    if !*admission_converged {
        ctx.converge_explicit_admission(desired);
        *admission_converged = true;
    }
    ctx.prune_tracked_maps_to_desired(desired);
    ctx.refresh_explicit_admission_metrics(desired);
    set_market_data_geyser_explicit_admitted_accounts(desired.len());
    set_market_data_geyser_explicit_cap_overflow(desired.cap_overflow());
    set_market_data_geyser_explicit_set_size(desired.len());

    let after_keys = desired.snapshot_pubkeys();
    debug_assert!(
        after_keys.len() <= desired.max_explicit_pubkeys(),
        "admitted SSOT must never exceed cap"
    );
    let delta = symmetric_diff(&before_keys, &after_keys);
    if delta.is_empty() && !continue_evict && !ctx.pending_geyser_evict() {
        inc_market_data_geyser_sync_skipped_no_delta_total();
        set_market_data_geyser_sync_pending(0);
        ctx.publish_admitted_explicit_physical(desired);
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
        ctx.continue_geyser_evict_with_deadline(flush_deadline, desired)
    } else {
        ctx.sync_geyser_tracked_accounts_batched_flush_with_deadline(flush_deadline, desired)
    };
    set_market_data_geyser_sync_pending(0);
    if release_flush_slot {
        ctx.release_geyser_sync_flush_slot();
    }
    record_market_data_md_state_sync_flush_duration_us(
        flush_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
    );
    ctx.prune_tracked_maps_to_desired(desired);
    ctx.publish_admitted_explicit_physical(desired);
    ctx.refresh_tracked_membership_snapshot();
    *ctx.last_synced_explicit_pubkeys_write() = after_keys;
    if !sync_complete {
        return false;
    }
    ctx.clear_pending_geyser_evict();
    true
}

pub fn consumer_id_for_track_pin(
    pin: super::worker::TrackPinReason,
) -> super::desired_set::ConsumerId {
    match pin {
        super::worker::TrackPinReason::Wallet => super::desired_set::ConsumerId::Wallet,
        super::worker::TrackPinReason::MomentumActive => super::desired_set::ConsumerId::Momentum,
        super::worker::TrackPinReason::ArbMultiDex => super::desired_set::ConsumerId::Arb,
    }
}
