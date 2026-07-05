//! PoolCacheUpdate JetStream metadata helpers shared by md-sidefx handlers.

use crate::execution::live_pool_cache::CachedPoolState;
use crate::execution::live_pool_cache::{
    MeteoraCpmmState, MeteoraState, OrcaWhirlpoolState, RaydiumCpmmState,
};
use crate::ipc::{
    DexPoolReadiness, NATIVE_SOL_MINT, POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY,
    POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY, POOL_CACHE_UPDATE_METEORA_DLMM_ONCHAIN_MINTS_KEY,
    POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY, POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY,
    POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY, POOL_CACHE_UPDATE_ORCA_ONCHAIN_MINTS_KEY,
    POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY, POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY,
    POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY, POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY,
    POOL_CACHE_UPDATE_ORCA_TOKEN_A_PROGRAM_KEY, POOL_CACHE_UPDATE_ORCA_TOKEN_B_PROGRAM_KEY,
    POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY,
};
use crate::solana::dex::pumpfun_amm::{
    pump_amm_sell_extended_layout_ready, PumpAmmSellExtendedReadinessParams,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;

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
pub fn meteora_dlmm_metadata_for_pool_cache_update(s: &MeteoraState) -> HashMap<String, String> {
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
        CachedPoolState::PumpFun(_) => None,
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
        CachedPoolState::PumpFun(_) => false,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::live_pool_cache::RaydiumCpmmState;

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
}
