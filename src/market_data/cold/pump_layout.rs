//! PumpSwap SELL-layout helpers for EnsurePumpAmmPoolAccounts publish path.

use crate::ipc::{ControlResponseStatus, DexPoolReadiness};
use crate::solana::dex::pumpfun_amm::{
    pump_amm_sell_extended_layout_ready, PumpAmmSellExtendedReadinessParams,
};
use solana_sdk::pubkey::Pubkey;

/// PumpSwap SELL-layout contract for JetStream / SLAVE SSOT.
///
/// - Base-only SELL stays ready as long as the underlying refresh/observation says the base layout is usable.
/// - Extended SELL is ready when pool `third_meta` (+ fee tails when required) is known; #21/#22 are derived at build.
#[allow(clippy::too_many_arguments)]
pub fn pump_amm_sell_layout_publish_state(
    sell_requires_extended: bool,
    sell_cashback_third_meta: Option<Pubkey>,
    sell_extended_tail_0: Option<Pubkey>,
    sell_extended_tail_1: Option<Pubkey>,
    sell_extended_fee_tail_0: Option<Pubkey>,
    sell_extended_fee_tail_1: Option<Pubkey>,
    sell_requires_fee_tail: bool,
    sell_requires_pre_fee_metas: bool,
    sell_pre_fee_meta_1: Option<Pubkey>,
    base_layout_ready: bool,
) -> (bool, DexPoolReadiness) {
    let sell_layout_ready = if sell_requires_extended {
        let _ = (sell_extended_tail_0, sell_extended_tail_1);
        pump_amm_sell_extended_layout_ready(PumpAmmSellExtendedReadinessParams {
            sell_requires_extended: true,
            third_meta: sell_cashback_third_meta,
            fee_tail_0: sell_extended_fee_tail_0,
            fee_tail_1: sell_extended_fee_tail_1,
            sell_requires_fee_tail,
            sell_requires_pre_fee_metas,
            sell_pre_fee_meta_1,
        }) && base_layout_ready
    } else {
        base_layout_ready
    };
    let dex_readiness = if sell_layout_ready {
        DexPoolReadiness::Ready
    } else {
        DexPoolReadiness::Partial
    };
    (sell_layout_ready, dex_readiness)
}

/// Resolve PumpSwap SELL-layout state for the EnsurePumpAmmPoolAccounts publish path.
///
/// `force_refresh=true` is authoritative and must be able to override stale monotonic cache hints.
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn pump_amm_sell_layout_state_for_ensure_publish(
    force_refresh: bool,
    cached_requires_extended: bool,
    cached_third_meta: Option<Pubkey>,
    cached_tail_0: Option<Pubkey>,
    cached_tail_1: Option<Pubkey>,
    cached_fee_tail_0: Option<Pubkey>,
    cached_fee_tail_1: Option<Pubkey>,
    cached_requires_fee_tail: bool,
    cached_requires_pre_fee_metas: bool,
    cached_pre_fee_meta_1: Option<Pubkey>,
    refresh_requires_extended: bool,
    refresh_third_meta: Option<Pubkey>,
    refresh_tail_0: Option<Pubkey>,
    refresh_tail_1: Option<Pubkey>,
    refresh_fee_tail_0: Option<Pubkey>,
    refresh_fee_tail_1: Option<Pubkey>,
    refresh_requires_fee_tail: bool,
    refresh_requires_pre_fee_metas: bool,
    refresh_pre_fee_meta_1: Option<Pubkey>,
    refresh_layout_ready: bool,
) -> (
    bool,
    Option<Pubkey>,
    Option<Pubkey>,
    Option<Pubkey>,
    Option<Pubkey>,
    Option<Pubkey>,
    bool,
    bool,
    Option<Pubkey>,
    bool,
    DexPoolReadiness,
) {
    let (
        effective_requires_extended,
        effective_third_meta,
        effective_tail_0,
        effective_tail_1,
        effective_fee_tail_0,
        effective_fee_tail_1,
        effective_requires_fee_tail,
        effective_requires_pre_fee_metas,
        effective_pre_fee_meta_1,
        base_layout_ready,
    ) = if force_refresh {
        (
            refresh_requires_extended || cached_requires_extended,
            refresh_third_meta.filter(|p| *p != Pubkey::default()),
            refresh_tail_0.filter(|p| *p != Pubkey::default()),
            refresh_tail_1.filter(|p| *p != Pubkey::default()),
            refresh_fee_tail_0
                .or(cached_fee_tail_0)
                .filter(|p| *p != Pubkey::default()),
            refresh_fee_tail_1
                .or(cached_fee_tail_1)
                .filter(|p| *p != Pubkey::default()),
            refresh_requires_fee_tail || cached_requires_fee_tail,
            refresh_requires_pre_fee_metas,
            if refresh_requires_pre_fee_metas {
                refresh_pre_fee_meta_1
                    .or(cached_pre_fee_meta_1)
                    .filter(|p| *p != Pubkey::default())
            } else {
                None
            },
            refresh_layout_ready,
        )
    } else {
        (
            cached_requires_extended || refresh_requires_extended,
            refresh_third_meta
                .or(cached_third_meta)
                .filter(|p| *p != Pubkey::default()),
            refresh_tail_0
                .or(cached_tail_0)
                .filter(|p| *p != Pubkey::default()),
            refresh_tail_1
                .or(cached_tail_1)
                .filter(|p| *p != Pubkey::default()),
            refresh_fee_tail_0
                .or(cached_fee_tail_0)
                .filter(|p| *p != Pubkey::default()),
            refresh_fee_tail_1
                .or(cached_fee_tail_1)
                .filter(|p| *p != Pubkey::default()),
            cached_requires_fee_tail || refresh_requires_fee_tail,
            cached_requires_pre_fee_metas || refresh_requires_pre_fee_metas,
            refresh_pre_fee_meta_1
                .or(cached_pre_fee_meta_1)
                .filter(|p| *p != Pubkey::default()),
            refresh_layout_ready,
        )
    };
    let (sell_layout_ready, dex_readiness) = pump_amm_sell_layout_publish_state(
        effective_requires_extended,
        effective_third_meta,
        effective_tail_0,
        effective_tail_1,
        effective_fee_tail_0,
        effective_fee_tail_1,
        effective_requires_fee_tail,
        effective_requires_pre_fee_metas,
        effective_pre_fee_meta_1,
        base_layout_ready,
    );
    (
        effective_requires_extended,
        effective_third_meta,
        effective_tail_0,
        effective_tail_1,
        effective_fee_tail_0,
        effective_fee_tail_1,
        effective_requires_fee_tail,
        effective_requires_pre_fee_metas,
        effective_pre_fee_meta_1,
        sell_layout_ready,
        dex_readiness,
    )
}

/// Control-response contract for PumpSwap EnsurePumpAmmPoolAccounts.
///
/// Only the authoritative `force_refresh=true` path may turn a successful JetStream publish into an
/// error when the resolved SELL layout is still not ready.
pub fn pump_amm_control_response_for_ensure_publish(
    force_refresh: bool,
    jetstream_ok: bool,
    sell_layout_ready: bool,
) -> (ControlResponseStatus, Option<String>) {
    if !jetstream_ok {
        return (
            ControlResponseStatus::Error,
            Some("JetStream publish failed".to_string()),
        );
    }
    if force_refresh && !sell_layout_ready {
        return (
            ControlResponseStatus::Error,
            Some("authoritative PumpSwap SELL layout unresolved after force_refresh".to_string()),
        );
    }
    (ControlResponseStatus::Ok, None)
}
