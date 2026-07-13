//! Phase 5a: track-worker Geyser explicit-set rebuild + coalesced delta-only push.

use super::admission_wiring::{
    admitted_pubkey_set, converge_admission_from_groups, merge_admission_tracker_owner_groups,
    rows_to_owner_groups, AdmissionConvergeResult,
};
use super::desired_set::symmetric_diff;
use super::explicit_admission::FixedCapAdmission;
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

pub fn converge_admission_from_ctx<C: TrackWorkerContext>(
    ctx: &C,
    admission: &mut FixedCapAdmission,
) -> AdmissionConvergeResult {
    let rows = ctx.explicit_pubkey_rows_for_desired_set();
    let mut groups = rows_to_owner_groups(&rows);
    merge_admission_tracker_owner_groups(admission, &mut groups);
    let result = converge_admission_from_groups(admission, &groups);
    set_market_data_geyser_explicit_admitted_accounts(admission.len());
    set_market_data_geyser_explicit_set_size(admission.len());
    ctx.on_admission_converge_result(admission, result);
    result
}

pub fn track_worker_execute_coalesced_push<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    admission: &mut FixedCapAdmission,
    before_keys: HashSet<Pubkey>,
    continue_evict: bool,
    release_flush_slot: bool,
    restore_barrier_pending: bool,
) -> bool {
    let flush_start = Instant::now();
    let converge = converge_admission_from_ctx(ctx.as_ref(), admission);
    let admitted = admitted_pubkey_set(admission);
    set_market_data_geyser_explicit_admitted_accounts(admitted.len());
    set_market_data_geyser_explicit_set_size(admitted.len());
    set_market_data_geyser_explicit_cap_overflow(
        if matches!(
            converge,
            AdmissionConvergeResult::ProtectedOverflow | AdmissionConvergeResult::Unconverged
        ) {
            1
        } else {
            0
        },
    );

    if !ctx.geyser_explicit_readiness_ok() {
        if release_flush_slot {
            ctx.release_geyser_sync_flush_slot();
        }
        if restore_barrier_pending {
            ctx.signal_restore_barrier(false);
        }
        return false;
    }

    if !matches!(converge, AdmissionConvergeResult::Converged) {
        if release_flush_slot {
            ctx.release_geyser_sync_flush_slot();
        }
        if restore_barrier_pending {
            ctx.signal_restore_barrier(false);
        }
        return false;
    }

    ctx.prune_tracked_maps_to_admitted(admission);
    let after_keys = admitted;
    let delta = symmetric_diff(&before_keys, &after_keys);
    if delta.is_empty() && !continue_evict && !ctx.pending_geyser_evict() {
        inc_market_data_geyser_sync_skipped_no_delta_total();
        set_market_data_geyser_sync_pending(0);
        ctx.publish_admitted_explicit_physical(admission);
        *ctx.last_synced_explicit_pubkeys_write() = after_keys;
        if release_flush_slot {
            ctx.release_geyser_sync_flush_slot();
        }
        if restore_barrier_pending {
            ctx.signal_restore_barrier(true);
        }
        return true;
    }
    if !delta.is_empty() {
        record_market_data_geyser_subscribe_delta_pubkeys(delta.len() as u64);
    }
    let flush_deadline =
        Instant::now() + Duration::from_millis(MARKET_DATA_MD_STATE_FLUSH_BUDGET_MS);
    let sync_complete = if continue_evict {
        ctx.continue_geyser_evict_with_deadline(flush_deadline, admission)
    } else {
        ctx.sync_geyser_tracked_accounts_batched_flush_with_deadline(flush_deadline, admission)
    };
    set_market_data_geyser_sync_pending(0);
    if release_flush_slot {
        ctx.release_geyser_sync_flush_slot();
    }
    record_market_data_md_state_sync_flush_duration_us(
        flush_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
    );
    ctx.prune_tracked_maps_to_admitted(admission);
    ctx.publish_admitted_explicit_physical(admission);
    ctx.refresh_tracked_membership_snapshot();
    *ctx.last_synced_explicit_pubkeys_write() = admitted_pubkey_set(admission);
    if !sync_complete {
        if restore_barrier_pending {
            ctx.signal_restore_barrier(false);
        }
        return false;
    }
    ctx.clear_pending_geyser_evict();
    if restore_barrier_pending {
        ctx.signal_restore_barrier(true);
    }
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
