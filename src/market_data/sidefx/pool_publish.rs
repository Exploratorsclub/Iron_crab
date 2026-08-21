//! PoolCacheUpdate JetStream metadata helpers shared by md-sidefx handlers.

use crate::execution::live_pool_cache::CachedPoolState;
use crate::execution::live_pool_cache::{
    MeteoraCpmmState, MeteoraState, OrcaWhirlpoolState, RaydiumAmmState, RaydiumCpmmState,
};
use crate::ipc::{
    DexPoolReadiness, NATIVE_SOL_MINT, POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY,
    POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY,
    POOL_CACHE_UPDATE_METEORA_DLMM_HAS_BITMAP_EXTENSION_KEY,
    POOL_CACHE_UPDATE_METEORA_DLMM_ONCHAIN_MINTS_KEY, POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY,
    POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY, POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY,
    POOL_CACHE_UPDATE_ORCA_ONCHAIN_MINTS_KEY, POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY,
    POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY, POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY,
    POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY, POOL_CACHE_UPDATE_ORCA_TOKEN_A_PROGRAM_KEY,
    POOL_CACHE_UPDATE_ORCA_TOKEN_B_PROGRAM_KEY, POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY,
};
use crate::solana::dex::pumpfun_amm::{
    pump_amm_sell_extended_layout_ready, PumpAmmSellExtendedReadinessParams,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;

/// Raydium AMM v4: Serum/OpenBook static metadata for JetStream SLAVE bootstrap.
pub fn raydium_amm_metadata_for_pool_cache_update(s: &RaydiumAmmState) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    if s.market_id != Pubkey::default() {
        meta.insert("market_id".to_string(), s.market_id.to_string());
    }
    if let (Some(bids), Some(asks), Some(eq)) = (s.serum_bids, s.serum_asks, s.serum_event_queue) {
        meta.insert("serum_bids".to_string(), bids.to_string());
        meta.insert("serum_asks".to_string(), asks.to_string());
        meta.insert("serum_event_queue".to_string(), eq.to_string());
    }
    if let Some(bv) = s.serum_base_vault {
        meta.insert("serum_base_vault".to_string(), bv.to_string());
    }
    if let Some(qv) = s.serum_quote_vault {
        meta.insert("serum_quote_vault".to_string(), qv.to_string());
    }
    meta
}

/// Preserve Serum/OpenBook static fields when Geyser pool parse omits them.
pub fn merge_raydium_amm_serum_fields_from_prior(
    new_am: &mut RaydiumAmmState,
    ex: &RaydiumAmmState,
) {
    if new_am.market_id == Pubkey::default() && ex.market_id != Pubkey::default() {
        new_am.market_id = ex.market_id;
    }
    if new_am.serum_bids.is_none() {
        new_am.serum_bids = ex.serum_bids;
    }
    if new_am.serum_asks.is_none() {
        new_am.serum_asks = ex.serum_asks;
    }
    if new_am.serum_event_queue.is_none() {
        new_am.serum_event_queue = ex.serum_event_queue;
    }
    if new_am.serum_base_vault.is_none() {
        new_am.serum_base_vault = ex.serum_base_vault;
    }
    if new_am.serum_quote_vault.is_none() {
        new_am.serum_quote_vault = ex.serum_quote_vault;
    }
}

/// Raydium CPMM: vault pubkeys in normalized base/quote order for JetStream metadata.
pub fn raydium_cpmm_vaults_for_pool_cache_update(s: &RaydiumCpmmState) -> String {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
    let (first_vault, second_vault) = if s.token_1_mint == sol {
        (s.token_0_vault, s.token_1_vault)
    } else if s.token_0_mint == sol {
        (s.token_1_vault, s.token_0_vault)
    } else {
        (s.token_0_vault, s.token_1_vault)
    };
    format!("{first_vault},{second_vault}")
}

/// Meteora CPMM: same normalized base/quote vault ordering as Raydium CPMM for JetStream metadata.
pub fn meteora_cpmm_vaults_for_pool_cache_update(s: &MeteoraCpmmState) -> String {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
    let (first_vault, second_vault) = if s.token_1_mint == sol {
        (s.token_0_vault, s.token_1_vault)
    } else if s.token_0_mint == sol {
        (s.token_1_vault, s.token_0_vault)
    } else {
        (s.token_0_vault, s.token_1_vault)
    };
    format!("{first_vault},{second_vault}")
}

/// On-chain `token_0_mint,token_1_mint` for SLAVE bootstrap when JetStream uses normalized base/quote.
pub fn meteora_cpmm_onchain_mints_for_pool_cache_update(s: &MeteoraCpmmState) -> String {
    format!("{},{}", s.token_0_mint, s.token_1_mint)
}

fn orca_onchain_mints_for_pool_cache_update(s: &OrcaWhirlpoolState) -> String {
    format!("{},{}", s.token_mint_a, s.token_mint_b)
}

/// Orca Whirlpool: PoolCacheUpdate metadata keys for SLAVE bootstrap.
pub fn orca_metadata_for_pool_cache_update(s: &OrcaWhirlpoolState) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY.to_string(),
        format!("{},{}", s.token_vault_a, s.token_vault_b),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_ONCHAIN_MINTS_KEY.to_string(),
        orca_onchain_mints_for_pool_cache_update(s),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY.to_string(),
        s.tick_current_index.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY.to_string(),
        s.tick_spacing.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY.to_string(),
        s.sqrt_price.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY.to_string(),
        s.liquidity.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY.to_string(),
        s.fee_rate.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY.to_string(),
        s.protocol_fee_rate.to_string(),
    );
    if let Some(p) = s.token_a_program {
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_TOKEN_A_PROGRAM_KEY.to_string(),
            p.to_string(),
        );
    }
    if let Some(p) = s.token_b_program {
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_TOKEN_B_PROGRAM_KEY.to_string(),
            p.to_string(),
        );
    }
    meta
}

fn meteora_dlmm_onchain_mints_for_pool_cache_update(s: &MeteoraState) -> String {
    format!("{},{}", s.token_x_mint, s.token_y_mint)
}

/// Meteora DLMM: vault pubkeys + static pool fields for JetStream / SLAVE bootstrap.
pub fn meteora_dlmm_metadata_for_pool_cache_update(
    s: &MeteoraState,
    has_bitmap_extension: Option<bool>,
) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert(
        POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY.to_string(),
        format!("{},{}", s.reserve_x, s.reserve_y),
    );
    meta.insert(
        POOL_CACHE_UPDATE_METEORA_DLMM_ONCHAIN_MINTS_KEY.to_string(),
        meteora_dlmm_onchain_mints_for_pool_cache_update(s),
    );
    meta.insert(
        POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY.to_string(),
        s.active_id.to_string(),
    );
    meta.insert(
        POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY.to_string(),
        s.bin_step.to_string(),
    );
    if let Some(has) = has_bitmap_extension {
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_DLMM_HAS_BITMAP_EXTENSION_KEY.to_string(),
            has.to_string(),
        );
    }
    meta
}

/// JetStream readiness for Raydium CPMM (SOL-aware base/quote).
pub fn raydium_cpmm_readiness_for_pool_cache_update(s: &RaydiumCpmmState) -> DexPoolReadiness {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
    let r0 = s.reserve_0.unwrap_or(0);
    let r1 = s.reserve_1.unwrap_or(0);
    let (base_side_liq, quote_side_liq) = if s.token_1_mint == sol {
        (r0 > 0, r1 > 0)
    } else if s.token_0_mint == sol {
        (r1 > 0, r0 > 0)
    } else {
        (r0 > 0, r1 > 0)
    };
    if base_side_liq && quote_side_liq {
        DexPoolReadiness::Ready
    } else if r0 > 0 || r1 > 0 {
        DexPoolReadiness::Partial
    } else {
        DexPoolReadiness::Observed
    }
}

/// PumpSwap SELL-layout contract for JetStream / SLAVE SSOT.
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

/// Extract normalized balance fields from MASTER cache for JetStream BalanceUpdated.
pub fn pool_cache_balance_fields_from_state(
    state: &CachedPoolState,
) -> Option<(Pubkey, Pubkey, u64, u64, &'static str)> {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).ok()?;
    match state {
        CachedPoolState::RaydiumCpmm(s) => {
            let (base_mint, quote_mint, base_r, quote_r) = if s.token_1_mint == sol {
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
            };
            Some((base_mint, quote_mint, base_r, quote_r, "raydium_cpmm"))
        }
        CachedPoolState::MeteoraCpmm(s) => {
            let (base_mint, quote_mint, base_r, quote_r) = if s.token_1_mint == sol {
                (s.token_0_mint, s.token_1_mint, s.reserve_0, s.reserve_1)
            } else if s.token_0_mint == sol {
                (s.token_1_mint, s.token_0_mint, s.reserve_1, s.reserve_0)
            } else {
                (s.token_0_mint, s.token_1_mint, s.reserve_0, s.reserve_1)
            };
            Some((base_mint, quote_mint, base_r, quote_r, "meteora_cpmm"))
        }
        CachedPoolState::Meteora(s) => {
            let (base_mint, quote_mint, base_r, quote_r) = if s.token_y_mint == sol {
                (
                    s.token_x_mint,
                    s.token_y_mint,
                    s.reserve_x_balance.unwrap_or(0),
                    s.reserve_y_balance.unwrap_or(0),
                )
            } else if s.token_x_mint == sol {
                (
                    s.token_y_mint,
                    s.token_x_mint,
                    s.reserve_y_balance.unwrap_or(0),
                    s.reserve_x_balance.unwrap_or(0),
                )
            } else {
                (
                    s.token_x_mint,
                    s.token_y_mint,
                    s.reserve_x_balance.unwrap_or(0),
                    s.reserve_y_balance.unwrap_or(0),
                )
            };
            Some((base_mint, quote_mint, base_r, quote_r, "meteora_dlmm"))
        }
        CachedPoolState::PumpAmm(s) => Some((
            s.base_mint,
            s.quote_mint,
            s.base_reserve.unwrap_or(0),
            s.quote_reserve.unwrap_or(0),
            "pump_amm",
        )),
        CachedPoolState::Orca(s) => {
            let (base_mint, quote_mint, base_r, quote_r) = if s.token_mint_b == sol {
                (
                    s.token_mint_a,
                    s.token_mint_b,
                    s.vault_a_balance.unwrap_or(0),
                    s.vault_b_balance.unwrap_or(0),
                )
            } else if s.token_mint_a == sol {
                (
                    s.token_mint_b,
                    s.token_mint_a,
                    s.vault_b_balance.unwrap_or(0),
                    s.vault_a_balance.unwrap_or(0),
                )
            } else {
                (
                    s.token_mint_a,
                    s.token_mint_b,
                    s.vault_a_balance.unwrap_or(0),
                    s.vault_b_balance.unwrap_or(0),
                )
            };
            Some((base_mint, quote_mint, base_r, quote_r, "orca"))
        }
        CachedPoolState::RaydiumAmm(s) => Some((
            s.base_mint,
            s.quote_mint,
            s.coin_reserve.unwrap_or(0),
            s.pc_reserve.unwrap_or(0),
            "raydium",
        )),
        CachedPoolState::PumpFun(s) => {
            let wsol = Pubkey::from_str(NATIVE_SOL_MINT).ok()?;
            Some((
                s.token_mint,
                wsol,
                s.virtual_token_reserves,
                s.virtual_sol_reserves,
                "pumpfun",
            ))
        }
    }
}

/// True when the cached pool row has at least one non-zero reserve / vault balance.
pub fn cached_pool_has_fresh_reserve_basis(state: &CachedPoolState) -> bool {
    let fresh = |opt: Option<u64>| opt.is_some_and(|v| v > 0);
    let fresh_u64 = |v: u64| v > 0;
    match state {
        CachedPoolState::RaydiumCpmm(s) => fresh(s.reserve_0) || fresh(s.reserve_1),
        CachedPoolState::MeteoraCpmm(s) => fresh_u64(s.reserve_0) || fresh_u64(s.reserve_1),
        CachedPoolState::Meteora(s) => fresh(s.reserve_x_balance) || fresh(s.reserve_y_balance),
        CachedPoolState::PumpAmm(s) => fresh(s.base_reserve) || fresh(s.quote_reserve),
        CachedPoolState::Orca(s) => fresh(s.vault_a_balance) || fresh(s.vault_b_balance),
        CachedPoolState::RaydiumAmm(s) => fresh(s.coin_reserve) || fresh(s.pc_reserve),
        CachedPoolState::PumpFun(s) => {
            !s.complete && s.virtual_sol_reserves > 0 && s.virtual_token_reserves > 0
        }
    }
}

/// Compare normalized reserve fields between two cache rows.
pub fn cache_balance_fields_unchanged(prev: &CachedPoolState, new: &CachedPoolState) -> bool {
    match (
        pool_cache_balance_fields_from_state(prev),
        pool_cache_balance_fields_from_state(new),
    ) {
        (Some((_, _, pb, pq, _)), Some((_, _, nb, nq, _))) => pb == nb && pq == nq,
        _ => false,
    }
}

/// True when parsed pool layout metadata materially changed (ENRICH publish gate, Scope D).
///
/// Compares vault pubkeys, mints, Serum/DLMM/Orca layout fields — not reserve balances
/// (those are gated separately via [`cache_balance_fields_unchanged`]).
pub fn pool_cache_state_layout_significant_change(
    prev: &CachedPoolState,
    new: &CachedPoolState,
) -> bool {
    if std::mem::discriminant(prev) != std::mem::discriminant(new) {
        return true;
    }
    match (prev, new) {
        (CachedPoolState::RaydiumAmm(p), CachedPoolState::RaydiumAmm(n)) => {
            p.market_id != n.market_id
                || p.coin_vault != n.coin_vault
                || p.pc_vault != n.pc_vault
                || p.base_mint != n.base_mint
                || p.quote_mint != n.quote_mint
                || p.serum_bids != n.serum_bids
                || p.serum_asks != n.serum_asks
                || p.serum_event_queue != n.serum_event_queue
                || p.serum_base_vault != n.serum_base_vault
                || p.serum_quote_vault != n.serum_quote_vault
        }
        (CachedPoolState::RaydiumCpmm(p), CachedPoolState::RaydiumCpmm(n)) => {
            p.token_0_mint != n.token_0_mint
                || p.token_1_mint != n.token_1_mint
                || p.token_0_vault != n.token_0_vault
                || p.token_1_vault != n.token_1_vault
        }
        (CachedPoolState::MeteoraCpmm(p), CachedPoolState::MeteoraCpmm(n)) => {
            p.token_0_mint != n.token_0_mint
                || p.token_1_mint != n.token_1_mint
                || p.token_0_vault != n.token_0_vault
                || p.token_1_vault != n.token_1_vault
                || p.amm_config != n.amm_config
                || p.observation_key != n.observation_key
                || p.token_0_program != n.token_0_program
                || p.token_1_program != n.token_1_program
                || p.status != n.status
        }
        (CachedPoolState::Meteora(p), CachedPoolState::Meteora(n)) => {
            p.active_id != n.active_id
                || p.bin_step != n.bin_step
                || p.token_x_mint != n.token_x_mint
                || p.token_y_mint != n.token_y_mint
                || p.reserve_x != n.reserve_x
                || p.reserve_y != n.reserve_y
        }
        (CachedPoolState::PumpAmm(p), CachedPoolState::PumpAmm(n)) => {
            p.pool_accounts != n.pool_accounts
                || p.pool_base_token_account != n.pool_base_token_account
                || p.pool_quote_token_account != n.pool_quote_token_account
                || p.base_mint != n.base_mint
                || p.quote_mint != n.quote_mint
                || p.creator != n.creator
        }
        (CachedPoolState::PumpFun(p), CachedPoolState::PumpFun(n)) => {
            p.complete != n.complete
                || p.real_token_reserves != n.real_token_reserves
                || p.real_sol_reserves != n.real_sol_reserves
                || p.creator != n.creator
                || p.token_mint != n.token_mint
                || p.associated_bonding_curve != n.associated_bonding_curve
                || p.cashback_enabled != n.cashback_enabled
        }
        (CachedPoolState::Orca(p), CachedPoolState::Orca(n)) => {
            p.tick_current_index != n.tick_current_index
                || p.sqrt_price != n.sqrt_price
                || p.token_mint_a != n.token_mint_a
                || p.token_mint_b != n.token_mint_b
                || p.token_vault_a != n.token_vault_a
                || p.token_vault_b != n.token_vault_b
                || p.tick_spacing != n.tick_spacing
                || p.fee_rate != n.fee_rate
                || p.protocol_fee_rate != n.protocol_fee_rate
                || p.liquidity != n.liquidity
                || p.token_a_program != n.token_a_program
                || p.token_b_program != n.token_b_program
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::live_pool_cache::RaydiumCpmmState;

    #[test]
    fn pumpfun_fresh_reserve_basis_requires_virtual_reserves() {
        use crate::execution::live_pool_cache::PumpFunState;

        let with = CachedPoolState::PumpFun(PumpFunState {
            token_mint: Pubkey::new_unique(),
            bonding_curve: Pubkey::new_unique(),
            associated_bonding_curve: Pubkey::new_unique(),
            virtual_sol_reserves: 1,
            virtual_token_reserves: 1,
            real_sol_reserves: 0,
            real_token_reserves: 0,
            complete: false,
            creator: Pubkey::new_unique(),
            cashback_enabled: false,
        });
        let without = CachedPoolState::PumpFun(PumpFunState {
            token_mint: Pubkey::new_unique(),
            bonding_curve: Pubkey::new_unique(),
            associated_bonding_curve: Pubkey::new_unique(),
            virtual_sol_reserves: 0,
            virtual_token_reserves: 0,
            real_sol_reserves: 0,
            real_token_reserves: 0,
            complete: false,
            creator: Pubkey::new_unique(),
            cashback_enabled: false,
        });
        assert!(cached_pool_has_fresh_reserve_basis(&with));
        assert!(!cached_pool_has_fresh_reserve_basis(&without));
    }

    #[test]
    fn pumpfun_balance_fields_from_state() {
        use crate::execution::live_pool_cache::PumpFunState;

        let mint = Pubkey::new_unique();
        let wsol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let state = CachedPoolState::PumpFun(PumpFunState {
            token_mint: mint,
            bonding_curve: Pubkey::new_unique(),
            associated_bonding_curve: Pubkey::new_unique(),
            virtual_sol_reserves: 30_000_000_000,
            virtual_token_reserves: 1_073_000_000_000_000,
            real_sol_reserves: 10_000_000_000,
            real_token_reserves: 500_000_000_000_000,
            complete: false,
            creator: Pubkey::new_unique(),
            cashback_enabled: false,
        });
        let (base, quote, base_r, quote_r, dex) =
            pool_cache_balance_fields_from_state(&state).expect("pumpfun fields");
        assert_eq!(base, mint);
        assert_eq!(quote, wsol);
        assert_eq!(base_r, 1_073_000_000_000_000);
        assert_eq!(quote_r, 30_000_000_000);
        assert_eq!(dex, "pumpfun");
    }

    #[test]
    fn cached_pool_has_fresh_reserve_basis_requires_nonzero() {
        let with = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: Pubkey::new_unique(),
            token_1_mint: Pubkey::new_unique(),
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: Some(1),
            reserve_1: None,
        });
        let without = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: Pubkey::new_unique(),
            token_1_mint: Pubkey::new_unique(),
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: None,
            reserve_1: None,
        });
        assert!(cached_pool_has_fresh_reserve_basis(&with));
        assert!(!cached_pool_has_fresh_reserve_basis(&without));
    }

    #[test]
    fn cache_balance_fields_unchanged_detects_equal_reserves() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let a = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: mint_a,
            token_1_mint: mint_b,
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: Some(100),
            reserve_1: Some(200),
        });
        let b = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: mint_a,
            token_1_mint: mint_b,
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: Some(100),
            reserve_1: Some(200),
        });
        let c = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: mint_a,
            token_1_mint: mint_b,
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: Some(101),
            reserve_1: Some(200),
        });
        assert!(cache_balance_fields_unchanged(&a, &b));
        assert!(!cache_balance_fields_unchanged(&a, &c));
    }

    #[test]
    fn layout_change_raydium_cpmm_vault_pubkey_is_significant() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let v0 = Pubkey::new_unique();
        let v1 = Pubkey::new_unique();
        let prev = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: mint_a,
            token_1_mint: mint_b,
            token_0_vault: v0,
            token_1_vault: v1,
            reserve_0: Some(100),
            reserve_1: Some(200),
        });
        let new_vault = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: mint_a,
            token_1_mint: mint_b,
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: v1,
            reserve_0: Some(100),
            reserve_1: Some(200),
        });
        assert!(pool_cache_state_layout_significant_change(
            &prev, &new_vault
        ));
        assert!(!pool_cache_state_layout_significant_change(&prev, &prev));
    }

    #[test]
    fn layout_change_raydium_cpmm_reserve_only_is_not_significant() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let v0 = Pubkey::new_unique();
        let v1 = Pubkey::new_unique();
        let prev = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: mint_a,
            token_1_mint: mint_b,
            token_0_vault: v0,
            token_1_vault: v1,
            reserve_0: Some(100),
            reserve_1: Some(200),
        });
        let new_bal = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: mint_a,
            token_1_mint: mint_b,
            token_0_vault: v0,
            token_1_vault: v1,
            reserve_0: Some(101),
            reserve_1: Some(200),
        });
        assert!(!pool_cache_state_layout_significant_change(&prev, &new_bal));
        assert!(!cache_balance_fields_unchanged(&prev, &new_bal));
    }

    #[test]
    fn layout_change_raydium_amm_serum_accounts_is_significant() {
        use crate::execution::live_pool_cache::RaydiumAmmState;

        let base = |serum_bids: Option<Pubkey>| {
            CachedPoolState::RaydiumAmm(RaydiumAmmState {
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                coin_vault: Pubkey::new_unique(),
                pc_vault: Pubkey::new_unique(),
                base_decimals: 6,
                quote_decimals: 9,
                coin_reserve: Some(1),
                pc_reserve: Some(2),
                market_id: Pubkey::new_unique(),
                serum_bids,
                serum_asks: Some(Pubkey::new_unique()),
                serum_event_queue: Some(Pubkey::new_unique()),
                serum_base_vault: None,
                serum_quote_vault: None,
            })
        };
        let prev = base(Some(Pubkey::new_unique()));
        let with_new_bids = base(Some(Pubkey::new_unique()));
        assert!(pool_cache_state_layout_significant_change(
            &prev,
            &with_new_bids
        ));
    }

    #[test]
    fn layout_change_meteora_dlmm_reserve_pubkeys_is_significant() {
        use crate::execution::live_pool_cache::MeteoraState;

        let mk = |reserve_x: Pubkey| {
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: Pubkey::new_unique(),
                token_y_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                reserve_x,
                reserve_y: Pubkey::new_unique(),
                active_id: 1,
                bin_step: 10,
                reserve_x_balance: Some(1),
                reserve_y_balance: Some(2),
            })
        };
        let prev = mk(Pubkey::new_unique());
        let next = mk(Pubkey::new_unique());
        assert!(pool_cache_state_layout_significant_change(&prev, &next));
    }

    #[test]
    fn raydium_amm_metadata_includes_serum_vaults_when_present() {
        use crate::execution::live_pool_cache::RaydiumAmmState;

        let market_id = Pubkey::new_unique();
        let bids = Pubkey::new_unique();
        let base_vault = Pubkey::new_unique();
        let quote_vault = Pubkey::new_unique();
        let s = RaydiumAmmState {
            base_mint: Pubkey::new_unique(),
            quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
            coin_vault: Pubkey::new_unique(),
            pc_vault: Pubkey::new_unique(),
            base_decimals: 9,
            quote_decimals: 9,
            coin_reserve: Some(1),
            pc_reserve: Some(2),
            market_id,
            serum_bids: Some(bids),
            serum_asks: Some(Pubkey::new_unique()),
            serum_event_queue: Some(Pubkey::new_unique()),
            serum_base_vault: Some(base_vault),
            serum_quote_vault: Some(quote_vault),
        };
        let meta = raydium_amm_metadata_for_pool_cache_update(&s);
        assert_eq!(
            meta.get("market_id").map(String::as_str),
            Some(market_id.to_string().as_str())
        );
        assert_eq!(
            meta.get("serum_base_vault").map(String::as_str),
            Some(base_vault.to_string().as_str())
        );
        assert_eq!(
            meta.get("serum_quote_vault").map(String::as_str),
            Some(quote_vault.to_string().as_str())
        );
    }
}
