//! Fresh Quote Calculator for LivePoolCache
//!
//! This module calculates fresh quotes from cached pool state, eliminating
//! the need for RPC calls during TX building. This is the core of Option C.
//!
//! # Usage
//!
//! ```ignore
//! let cache = create_shared_cache();
//! let intent = ...; // TradeIntent with pool and amounts
//!
//! // Calculate fresh min_out from live cache data
//! let fresh_min_out = calculate_fresh_min_out(&cache, &intent)?;
//! ```

use super::live_pool_cache::{
    CachedPoolState, MeteoraState, OrcaWhirlpoolState, PumpAmmState, PumpFunState,
    RaydiumAmmState, RaydiumCpmmState, SharedLivePoolCache,
};
use crate::ipc::{TradeIntent, TradeSide};
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

// ============================================================================
// Main API
// ============================================================================

/// Calculate fresh min_out from cache for an intent
///
/// Returns the calculated min_out after applying slippage.
/// Returns None if pool not in cache or calculation fails.
pub fn calculate_fresh_min_out(
    cache: &SharedLivePoolCache,
    intent: &TradeIntent,
) -> Result<Option<u64>> {
    // Get pool address from intent
    if intent.resources.pools.is_empty() {
        return Err(anyhow!("no pool in intent resources"));
    }
    let pool_str = &intent.resources.pools[0];
    let pool_id = Pubkey::from_str(pool_str)?;

    // Get cached pool state
    let (state, slot, age_ms) = match cache.get_with_metadata(&pool_id) {
        Some(data) => data,
        None => {
            tracing::debug!(pool = %pool_id, "quote_calc: pool not in cache");
            return Ok(None);
        }
    };

    // Warn if cache entry is stale (>5 seconds)
    if age_ms > 5000 {
        tracing::warn!(
            pool = %pool_id,
            age_ms,
            slot,
            "quote_calc: cache entry is stale, quote may be inaccurate"
        );
    }

    let amount_in = intent.required_capital.raw;
    let slippage_bps = intent.max_slippage_bps; // Already a u32

    // Determine direction based on mints
    let is_buy = intent.side == TradeSide::Buy;

    // Calculate quote based on DEX type
    let quote_result = match &state {
        CachedPoolState::Orca(s) => calculate_orca_quote(s, amount_in, intent, is_buy),
        CachedPoolState::RaydiumAmm(s) => calculate_raydium_amm_quote(s, amount_in, intent, is_buy),
        CachedPoolState::RaydiumCpmm(s) => {
            calculate_raydium_cpmm_quote(s, amount_in, intent, is_buy)
        }
        CachedPoolState::Meteora(s) => calculate_meteora_quote(s, amount_in, intent, is_buy),
        CachedPoolState::PumpFun(s) => calculate_pumpfun_quote(s, amount_in, is_buy),
        CachedPoolState::PumpAmm(s) => calculate_pumpamm_quote(s, amount_in, intent, is_buy),
    };

    match quote_result {
        Ok(amount_out) => {
            if amount_out == 0 {
                tracing::warn!(pool = %pool_id, dex = %state.dex_name(), "quote_calc: calculated zero output");
                return Ok(None);
            }

            // Apply slippage to get min_out
            let min_out = apply_slippage(amount_out, slippage_bps);

            tracing::debug!(
                pool = %pool_id,
                dex = %state.dex_name(),
                amount_in,
                amount_out,
                slippage_bps,
                min_out,
                cache_age_ms = age_ms,
                "quote_calc: calculated fresh min_out"
            );

            Ok(Some(min_out))
        }
        Err(e) => {
            tracing::warn!(
                pool = %pool_id,
                dex = %state.dex_name(),
                error = %e,
                "quote_calc: calculation failed"
            );
            Ok(None)
        }
    }
}

/// Apply slippage to get min_out
/// min_out = amount_out * (10000 - slippage_bps) / 10000
pub fn apply_slippage(amount_out: u64, slippage_bps: u32) -> u64 {
    if slippage_bps >= 10000 {
        return 0;
    }
    let keep = 10000u64 - slippage_bps as u64;
    ((amount_out as u128 * keep as u128) / 10000u128) as u64
}

// ============================================================================
// DEX-specific quote calculations
// ============================================================================

/// Calculate Orca Whirlpool quote using constant product formula
///
/// Orca uses concentrated liquidity, but for a rough estimate we use
/// the simple constant product formula with fee deduction.
fn calculate_orca_quote(
    state: &OrcaWhirlpoolState,
    amount_in: u64,
    intent: &TradeIntent,
    _is_buy: bool,
) -> Result<u64> {
    // Determine direction: a_to_b or b_to_a
    let input_mint = Pubkey::from_str(&intent.resources.input_mint)?;
    let a_to_b = input_mint == state.token_mint_a;

    // Get reserves from vault balances
    let (reserve_in, reserve_out) = if a_to_b {
        (
            state.vault_a_balance.unwrap_or(0) as u128,
            state.vault_b_balance.unwrap_or(0) as u128,
        )
    } else {
        (
            state.vault_b_balance.unwrap_or(0) as u128,
            state.vault_a_balance.unwrap_or(0) as u128,
        )
    };

    if reserve_in == 0 || reserve_out == 0 {
        return Err(anyhow!(
            "orca: missing vault balances (in={}, out={})",
            reserve_in,
            reserve_out
        ));
    }

    // Apply fee (fee_rate is in hundredths of a bps, so 3000 = 0.30% = 30 bps)
    let fee_bps = state.fee_rate as u128 / 100; // Convert to bps
    let amount_in_u128 = amount_in as u128;
    let amount_after_fee = amount_in_u128 * (10000 - fee_bps) / 10000;

    // Constant product: out = (in_after_fee * reserve_out) / (reserve_in + in_after_fee)
    let amount_out = (amount_after_fee * reserve_out) / (reserve_in + amount_after_fee);

    Ok(amount_out as u64)
}

/// Calculate Raydium AMM V4 quote using constant product formula
fn calculate_raydium_amm_quote(
    state: &RaydiumAmmState,
    amount_in: u64,
    intent: &TradeIntent,
    _is_buy: bool,
) -> Result<u64> {
    // Determine direction
    let input_mint = Pubkey::from_str(&intent.resources.input_mint)?;
    let base_to_quote = input_mint == state.base_mint;

    // Get reserves
    let (reserve_in, reserve_out) = if base_to_quote {
        (
            state.coin_reserve.unwrap_or(0) as u128,
            state.pc_reserve.unwrap_or(0) as u128,
        )
    } else {
        (
            state.pc_reserve.unwrap_or(0) as u128,
            state.coin_reserve.unwrap_or(0) as u128,
        )
    };

    if reserve_in == 0 || reserve_out == 0 {
        return Err(anyhow!(
            "raydium_amm: missing reserves (in={}, out={})",
            reserve_in,
            reserve_out
        ));
    }

    // Raydium AMM default fee: 25 bps (0.25%)
    const FEE_BPS: u128 = 25;
    let amount_in_u128 = amount_in as u128;
    let amount_after_fee = amount_in_u128 * (10000 - FEE_BPS) / 10000;

    // Constant product formula
    let k = reserve_in * reserve_out;
    let new_reserve_in = reserve_in + amount_after_fee;
    let new_reserve_out = k / new_reserve_in;
    let amount_out = reserve_out.saturating_sub(new_reserve_out);

    Ok(amount_out as u64)
}

/// Calculate Raydium CPMM quote using constant product formula
fn calculate_raydium_cpmm_quote(
    state: &RaydiumCpmmState,
    amount_in: u64,
    intent: &TradeIntent,
    _is_buy: bool,
) -> Result<u64> {
    // Determine direction
    let input_mint = Pubkey::from_str(&intent.resources.input_mint)?;
    let zero_to_one = input_mint == state.token_0_mint;

    // Get reserves
    let (reserve_in, reserve_out) = if zero_to_one {
        (
            state.reserve_0.unwrap_or(0) as u128,
            state.reserve_1.unwrap_or(0) as u128,
        )
    } else {
        (
            state.reserve_1.unwrap_or(0) as u128,
            state.reserve_0.unwrap_or(0) as u128,
        )
    };

    if reserve_in == 0 || reserve_out == 0 {
        return Err(anyhow!(
            "raydium_cpmm: missing reserves (in={}, out={})",
            reserve_in,
            reserve_out
        ));
    }

    // CPMM default fee: 25 bps
    const FEE_BPS: u128 = 25;
    let amount_in_u128 = amount_in as u128;
    let amount_after_fee = amount_in_u128 * (10000 - FEE_BPS) / 10000;

    // Constant product formula
    let k = reserve_in * reserve_out;
    let new_reserve_in = reserve_in + amount_after_fee;
    let new_reserve_out = k / new_reserve_in;
    let amount_out = reserve_out.saturating_sub(new_reserve_out);

    Ok(amount_out as u64)
}

/// Calculate Meteora DLMM quote
///
/// Meteora uses discrete liquidity bins. For a rough estimate, we use
/// constant product as an approximation. A more accurate implementation
/// would require bin array data.
fn calculate_meteora_quote(
    state: &MeteoraState,
    amount_in: u64,
    intent: &TradeIntent,
    _is_buy: bool,
) -> Result<u64> {
    // Determine direction
    let input_mint = Pubkey::from_str(&intent.resources.input_mint)?;
    let x_to_y = input_mint == state.token_x_mint;

    // Get reserves
    let (reserve_in, reserve_out) = if x_to_y {
        (
            state.reserve_x_balance.unwrap_or(0) as u128,
            state.reserve_y_balance.unwrap_or(0) as u128,
        )
    } else {
        (
            state.reserve_y_balance.unwrap_or(0) as u128,
            state.reserve_x_balance.unwrap_or(0) as u128,
        )
    };

    if reserve_in == 0 || reserve_out == 0 {
        return Err(anyhow!(
            "meteora: missing reserves (in={}, out={})",
            reserve_in,
            reserve_out
        ));
    }

    // Meteora fee depends on bin_step, but typical is ~30 bps
    // bin_step of 1 = ~0.01% per bin, typical pools have bin_step 10-100
    let fee_bps = (state.bin_step as u128).min(100); // Rough approximation
    let amount_in_u128 = amount_in as u128;
    let amount_after_fee = amount_in_u128 * (10000 - fee_bps) / 10000;

    // Use constant product as approximation
    let amount_out = (amount_after_fee * reserve_out) / (reserve_in + amount_after_fee);

    Ok(amount_out as u64)
}

/// Calculate PumpFun Bonding Curve quote
///
/// Uses the exact formula from the bonding curve: constant product with virtual reserves.
fn calculate_pumpfun_quote(state: &PumpFunState, amount_in: u64, is_buy: bool) -> Result<u64> {
    if state.complete {
        return Err(anyhow!("pumpfun: bonding curve is complete (migrated)"));
    }

    let amount_in_u128 = amount_in as u128;

    if is_buy {
        // BUY: SOL in, Token out
        let sol_reserve = state.virtual_sol_reserves as u128;
        let token_reserve = state.virtual_token_reserves as u128;

        if sol_reserve == 0 {
            return Err(anyhow!("pumpfun: sol_reserve is zero"));
        }

        // amount_out = (amount_in * token_reserve) / (sol_reserve + amount_in)
        let numerator = amount_in_u128 * token_reserve;
        let denominator = sol_reserve + amount_in_u128;
        let amount_out = numerator / denominator.max(1);

        Ok(amount_out as u64)
    } else {
        // SELL: Token in, SOL out
        let sol_reserve = state.virtual_sol_reserves as u128;
        let token_reserve = state.virtual_token_reserves as u128;

        if token_reserve == 0 {
            return Err(anyhow!("pumpfun: token_reserve is zero"));
        }

        // amount_out = (amount_in * sol_reserve) / (token_reserve + amount_in)
        let numerator = amount_in_u128 * sol_reserve;
        let denominator = token_reserve + amount_in_u128;
        let amount_out = numerator / denominator.max(1);

        Ok(amount_out as u64)
    }
}

/// Calculate PumpFun AMM (PumpSwap) quote
///
/// Uses constant product formula with 1% fee (100 bps).
fn calculate_pumpamm_quote(
    state: &PumpAmmState,
    amount_in: u64,
    intent: &TradeIntent,
    _is_buy: bool,
) -> Result<u64> {
    // Determine direction
    let input_mint = Pubkey::from_str(&intent.resources.input_mint)?;
    let base_to_quote = input_mint == state.base_mint;

    // Get reserves
    let (reserve_in, reserve_out) = if base_to_quote {
        (
            state.base_reserve.unwrap_or(0) as u128,
            state.quote_reserve.unwrap_or(0) as u128,
        )
    } else {
        (
            state.quote_reserve.unwrap_or(0) as u128,
            state.base_reserve.unwrap_or(0) as u128,
        )
    };

    if reserve_in == 0 || reserve_out == 0 {
        return Err(anyhow!(
            "pump_amm: missing reserves (in={}, out={})",
            reserve_in,
            reserve_out
        ));
    }

    // PumpSwap fee: 100 bps (1%)
    const FEE_BPS: u128 = 100;
    let amount_in_u128 = amount_in as u128;
    let amount_after_fee = amount_in_u128 * (10000 - FEE_BPS) / 10000;

    // Constant product formula
    let amount_out = (amount_after_fee * reserve_out) / (reserve_in + amount_after_fee);

    Ok(amount_out as u64)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_slippage() {
        // 5% slippage (500 bps)
        assert_eq!(apply_slippage(1000, 500), 950);

        // 1% slippage (100 bps)
        assert_eq!(apply_slippage(1000, 100), 990);

        // 0% slippage
        assert_eq!(apply_slippage(1000, 0), 1000);

        // 100% slippage (should return 0)
        assert_eq!(apply_slippage(1000, 10000), 0);
    }

    #[test]
    fn test_pumpfun_quote_buy() {
        let state = PumpFunState {
            token_mint: Pubkey::new_unique(),
            bonding_curve: Pubkey::new_unique(),
            associated_bonding_curve: Pubkey::new_unique(),
            virtual_sol_reserves: 30_000_000_000, // 30 SOL
            virtual_token_reserves: 1_000_000_000_000_000, // 1B tokens
            real_sol_reserves: 0,
            real_token_reserves: 793_100_000_000_000,
            complete: false,
            creator: Pubkey::new_unique(),
        };

        // Buy with 1 SOL
        let amount_out = calculate_pumpfun_quote(&state, 1_000_000_000, true).unwrap();
        // Expected: (1 * 1B) / (30 + 1) ≈ 32.26M tokens
        assert!(amount_out > 30_000_000_000_000);
        assert!(amount_out < 35_000_000_000_000);
    }

    #[test]
    fn test_pumpfun_quote_sell() {
        let state = PumpFunState {
            token_mint: Pubkey::new_unique(),
            bonding_curve: Pubkey::new_unique(),
            associated_bonding_curve: Pubkey::new_unique(),
            virtual_sol_reserves: 30_000_000_000, // 30 SOL
            virtual_token_reserves: 1_000_000_000_000_000, // 1B tokens
            real_sol_reserves: 0,
            real_token_reserves: 793_100_000_000_000,
            complete: false,
            creator: Pubkey::new_unique(),
        };

        // Sell 100M tokens
        let amount_out = calculate_pumpfun_quote(&state, 100_000_000_000_000, false).unwrap();
        // Expected: (100M * 30 SOL) / (1B + 100M) ≈ 2.73 SOL
        assert!(amount_out > 2_500_000_000);
        assert!(amount_out < 3_000_000_000);
    }

    #[test]
    fn test_constant_product_formula() {
        // Generic constant product: out = (in * reserve_out) / (reserve_in + in)
        let reserve_in: u128 = 1_000_000_000; // 1B
        let reserve_out: u128 = 1_000_000_000; // 1B
        let amount_in: u128 = 100_000_000; // 100M (10%)

        let amount_out = (amount_in * reserve_out) / (reserve_in + amount_in);

        // 10% of pool → ~9.09% out (due to price impact)
        assert_eq!(amount_out, 90909090);
    }
}
