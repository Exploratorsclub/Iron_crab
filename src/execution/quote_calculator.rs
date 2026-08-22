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
    CachedPoolState, MeteoraCpmmState, MeteoraState, OrcaWhirlpoolState, PumpAmmState,
    PumpFunState, RaydiumAmmState, RaydiumCpmmState, SharedLivePoolCache,
};
use crate::ipc::{TradeIntent, TradeSide};
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

/// Native SOL / WSOL mint (lamport-denominated input for PumpFun buys).
const WSOL_MINT_PK: Pubkey = solana_sdk::pubkey!("So11111111111111111111111111111111111111112");

/// Infer PumpFun swap direction when `input_mint` is the token being sold or SOL being spent.
fn pumpfun_is_buy_input(state: &PumpFunState, input_mint: &Pubkey) -> bool {
    if state.token_mint != Pubkey::default() {
        // SOL/WSOL in → buy tokens; position token mint in → sell for SOL.
        *input_mint != state.token_mint
    } else {
        // `parse_pumpfun_bonding` leaves `token_mint` default until market-data enrichment.
        // Without a known curve mint, only native SOL input is a buy; token input is a sell.
        *input_mint == WSOL_MINT_PK
    }
}

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
        CachedPoolState::MeteoraCpmm(s) => {
            calculate_meteora_cpmm_quote(s, amount_in, intent, is_buy)
        }
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

/// Calculate output amount for a pool given input amount and input_mint.
///
/// This is a lower-level API than `calculate_fresh_min_out` — it does NOT
/// require a `TradeIntent` and works directly with `CachedPoolState`.
/// Useful for multi-pool comparison in strategy bots (e.g. momentum-bot).
///
/// Returns `Ok(amount_out)` (before slippage) or an error if the pool
/// cannot provide a quote (e.g. zero reserves, completed PumpFun curve).
pub fn quote_output_amount(
    state: &CachedPoolState,
    amount_in: u64,
    input_mint: &Pubkey,
) -> Result<u64> {
    match state {
        CachedPoolState::PumpFun(s) => {
            let is_buy = pumpfun_is_buy_input(s, input_mint);
            calculate_pumpfun_quote(s, amount_in, is_buy)
        }
        CachedPoolState::PumpAmm(s) => {
            let base_to_quote = *input_mint == s.base_mint;
            let (reserve_in, reserve_out) = if base_to_quote {
                (
                    s.base_reserve.unwrap_or(0) as u128,
                    s.quote_reserve.unwrap_or(0) as u128,
                )
            } else {
                (
                    s.quote_reserve.unwrap_or(0) as u128,
                    s.base_reserve.unwrap_or(0) as u128,
                )
            };
            if reserve_in == 0 || reserve_out == 0 {
                return Err(anyhow!("pump_amm: missing reserves"));
            }
            const FEE_BPS: u128 = 100;
            let a = amount_in as u128;
            let after_fee = a * (10000 - FEE_BPS) / 10000;
            let out = (after_fee * reserve_out) / (reserve_in + after_fee);
            Ok(out as u64)
        }
        CachedPoolState::Orca(s) => {
            let a_to_b = *input_mint == s.token_mint_a;
            let (ri, ro) = if a_to_b {
                (
                    s.vault_a_balance.unwrap_or(0) as u128,
                    s.vault_b_balance.unwrap_or(0) as u128,
                )
            } else {
                (
                    s.vault_b_balance.unwrap_or(0) as u128,
                    s.vault_a_balance.unwrap_or(0) as u128,
                )
            };
            if ri == 0 || ro == 0 {
                return Err(anyhow!("orca: missing vault balances"));
            }
            let fee_bps = s.fee_rate as u128 / 100;
            let a = amount_in as u128;
            let after_fee = a * (10000 - fee_bps) / 10000;
            Ok(((after_fee * ro) / (ri + after_fee)) as u64)
        }
        CachedPoolState::RaydiumAmm(s) => {
            let base_to_quote = *input_mint == s.base_mint;
            let (ri, ro) = if base_to_quote {
                (
                    s.coin_reserve.unwrap_or(0) as u128,
                    s.pc_reserve.unwrap_or(0) as u128,
                )
            } else {
                (
                    s.pc_reserve.unwrap_or(0) as u128,
                    s.coin_reserve.unwrap_or(0) as u128,
                )
            };
            if ri == 0 || ro == 0 {
                return Err(anyhow!("raydium_amm: missing reserves"));
            }
            const FEE: u128 = 25;
            let a = amount_in as u128;
            let after_fee = a * (10000 - FEE) / 10000;
            let k = ri * ro;
            let new_ri = ri + after_fee;
            Ok(ro.saturating_sub(k / new_ri) as u64)
        }
        CachedPoolState::RaydiumCpmm(s) => {
            let zero_to_one = *input_mint == s.token_0_mint;
            let (ri, ro) = if zero_to_one {
                (
                    s.reserve_0.unwrap_or(0) as u128,
                    s.reserve_1.unwrap_or(0) as u128,
                )
            } else {
                (
                    s.reserve_1.unwrap_or(0) as u128,
                    s.reserve_0.unwrap_or(0) as u128,
                )
            };
            if ri == 0 || ro == 0 {
                return Err(anyhow!("raydium_cpmm: missing reserves"));
            }
            const FEE: u128 = 25;
            let a = amount_in as u128;
            let after_fee = a * (10000 - FEE) / 10000;
            let k = ri * ro;
            let new_ri = ri + after_fee;
            Ok(ro.saturating_sub(k / new_ri) as u64)
        }
        CachedPoolState::Meteora(s) => {
            let x_to_y = *input_mint == s.token_x_mint;
            let (ri, ro) = if x_to_y {
                (
                    s.reserve_x_balance.unwrap_or(0) as u128,
                    s.reserve_y_balance.unwrap_or(0) as u128,
                )
            } else {
                (
                    s.reserve_y_balance.unwrap_or(0) as u128,
                    s.reserve_x_balance.unwrap_or(0) as u128,
                )
            };
            if ri == 0 || ro == 0 {
                return Err(anyhow!("meteora: missing reserves"));
            }
            let fee_bps = (s.bin_step as u128).min(100);
            let a = amount_in as u128;
            let after_fee = a * (10000 - fee_bps) / 10000;
            Ok(((after_fee * ro) / (ri + after_fee)) as u64)
        }
        CachedPoolState::MeteoraCpmm(s) => {
            let is_token_0 = *input_mint == s.token_0_mint;
            let (ri, ro) = if is_token_0 {
                (s.reserve_0 as u128, s.reserve_1 as u128)
            } else {
                (s.reserve_1 as u128, s.reserve_0 as u128)
            };
            if ri == 0 || ro == 0 {
                return Err(anyhow!("meteora_cpmm: missing reserves"));
            }
            let fee: u128 = 25;
            let a = amount_in as u128;
            let fm = 10000 - fee;
            let num = ro * a * fm;
            let den = ri * 10000 + a * fm;
            if den == 0 {
                return Err(anyhow!("meteora_cpmm: denominator zero"));
            }
            Ok((num / den) as u64)
        }
    }
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

/// Calculate Meteora CPMM (DAMM V2) quote using constant product formula
///
/// This is simpler than DLMM - uses standard x*y=k formula.
fn calculate_meteora_cpmm_quote(
    state: &MeteoraCpmmState,
    amount_in: u64,
    intent: &TradeIntent,
    _is_buy: bool,
) -> Result<u64> {
    // Determine direction
    let input_mint = Pubkey::from_str(&intent.resources.input_mint)?;
    let is_token_0_input = input_mint == state.token_0_mint;

    // Get reserves
    let (reserve_in, reserve_out) = if is_token_0_input {
        (state.reserve_0 as u128, state.reserve_1 as u128)
    } else {
        (state.reserve_1 as u128, state.reserve_0 as u128)
    };

    if reserve_in == 0 || reserve_out == 0 {
        return Err(anyhow!(
            "meteora_cpmm: missing reserves (in={}, out={})",
            reserve_in,
            reserve_out
        ));
    }

    // Meteora CPMM default fee is 0.25% = 25 bps
    let fee_bps: u128 = 25;
    let amount_in_u128 = amount_in as u128;
    let fee_multiplier = 10000 - fee_bps;

    // amount_out = (reserve_out * amount_in * fee_multiplier) / (reserve_in * 10000 + amount_in * fee_multiplier)
    let numerator = reserve_out * amount_in_u128 * fee_multiplier;
    let denominator = reserve_in * 10000 + amount_in_u128 * fee_multiplier;

    if denominator == 0 {
        return Err(anyhow!("meteora_cpmm: denominator is zero"));
    }

    let amount_out = numerator / denominator;

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
        //
        // Validate against real_token_reserves before quoting.
        // PumpFun's on-chain sell function checks: `amount <= real_token_reserves`
        // (error 6023: NotEnoughTokensToSell).
        //
        // IMPORTANT: If real_reserves=0 but virtual_reserves are populated, the cache
        // may be stale (PoolCacheUpdate from older market-data without real_reserves).
        // In that case, skip the real_reserves validation and let simulation catch failures.
        let real_reserves_populated = state.real_token_reserves > 0 || state.real_sol_reserves > 0;
        if real_reserves_populated {
            if amount_in > state.real_token_reserves {
                return Err(anyhow!(
                    "pumpfun: sell amount {} exceeds real_token_reserves {} (curve cannot absorb this sell)",
                    amount_in,
                    state.real_token_reserves
                ));
            }
            if state.real_sol_reserves == 0 {
                return Err(anyhow!(
                    "pumpfun: real_sol_reserves is zero (curve has no SOL to pay out)"
                ));
            }
        }

        let sol_reserve = state.virtual_sol_reserves as u128;
        let token_reserve = state.virtual_token_reserves as u128;

        if token_reserve == 0 {
            return Err(anyhow!("pumpfun: token_reserve is zero"));
        }

        // amount_out = (amount_in * sol_reserve) / (token_reserve + amount_in)
        let numerator = amount_in_u128 * sol_reserve;
        let denominator = token_reserve + amount_in_u128;
        let amount_out = numerator / denominator.max(1);

        // Final check: ensure SOL output doesn't exceed what the curve actually has
        // (only if real_reserves are populated — otherwise cache is stale)
        if real_reserves_populated && amount_out as u64 > state.real_sol_reserves {
            return Err(anyhow!(
                "pumpfun: calculated sol_output {} exceeds real_sol_reserves {} (curve underfunded)",
                amount_out,
                state.real_sol_reserves
            ));
        }

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
            cashback_enabled: false,
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
            real_sol_reserves: 5_000_000_000,     // 5 SOL (curve has SOL to pay out)
            real_token_reserves: 793_100_000_000_000,
            complete: false,
            creator: Pubkey::new_unique(),
            cashback_enabled: false,
        };

        // Sell 100M tokens — formula uses virtual reserves; output capped by real_sol_reserves
        let amount_out = calculate_pumpfun_quote(&state, 100_000_000_000_000, false).unwrap();
        // Expected from virtual: (100M * 30 SOL) / (1B + 100M) ≈ 2.73 SOL; capped by real (5 SOL)
        assert!(amount_out > 2_500_000_000);
        assert!(amount_out <= 5_000_000_000); // cannot exceed real_sol_reserves
    }

    /// Prod-scale: missing `token_mint` in cache must not flip sell → buy (token out as lamports).
    #[test]
    fn pumpfun_sell_quote_tps_same_order_when_token_mint_missing_from_cache() {
        use crate::execution::tokens_per_sol;

        let token_mint = Pubkey::new_unique();
        let state = PumpFunState {
            token_mint: Pubkey::default(),
            bonding_curve: Pubkey::new_unique(),
            associated_bonding_curve: Pubkey::new_unique(),
            virtual_sol_reserves: 30_000_000_000,
            virtual_token_reserves: 1_000_000_000_000_000,
            real_sol_reserves: 5_000_000_000,
            real_token_reserves: 793_100_000_000_000,
            complete: false,
            creator: Pubkey::new_unique(),
            cashback_enabled: false,
        };
        let sell_raw = 69_000_000_000u64;
        let sol_out = quote_output_amount(&CachedPoolState::PumpFun(state), sell_raw, &token_mint)
            .expect("sell quote");
        let tps = tokens_per_sol::ui_tokens_per_sol(sell_raw, 6, sol_out);
        assert!(
            tps > 1_000_000.0 && tps < 100_000_000.0,
            "expected entry-scale tps (not ~0.17), got {tps}"
        );
        let entry_tps = 1.0e7;
        assert!(!tokens_per_sol::exit_quote_tps_scale_ratio_exceeded(
            entry_tps, tps, 100.0
        ));
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

    #[test]
    fn test_raydium_amm_quote_buy() {
        // Test Raydium AMM quote for buying token with SOL
        let wsol_mint = Pubkey::new_unique();
        let token_mint = Pubkey::new_unique();

        let state = RaydiumAmmState {
            base_mint: token_mint,
            quote_mint: wsol_mint,
            coin_vault: Pubkey::new_unique(),
            pc_vault: Pubkey::new_unique(),
            base_decimals: 9,
            quote_decimals: 9,
            coin_reserve: Some(10_000_000_000_000), // 10K tokens
            pc_reserve: Some(1_000_000_000_000),    // 1K SOL
            market_id: Pubkey::new_unique(),
            serum_bids: None,
            serum_asks: None,
            serum_event_queue: None,
            serum_base_vault: None,
            serum_quote_vault: None,
        };

        let intent = create_test_intent(wsol_mint, token_mint, 1_000_000_000); // 1 SOL
        let amount_out =
            super::calculate_raydium_amm_quote(&state, 1_000_000_000, &intent, true).unwrap();

        // 1 SOL in 1000 SOL pool → should get ~10 tokens (with fee and price impact)
        // Expected: ~9.97 tokens (with 0.25% fee and price impact)
        assert!(amount_out > 9_000_000_000);
        assert!(amount_out < 11_000_000_000);
    }

    #[test]
    fn test_raydium_amm_quote_sell() {
        // Test Raydium AMM quote for selling token for SOL
        let wsol_mint = Pubkey::new_unique();
        let token_mint = Pubkey::new_unique();

        let state = RaydiumAmmState {
            base_mint: token_mint,
            quote_mint: wsol_mint,
            coin_vault: Pubkey::new_unique(),
            pc_vault: Pubkey::new_unique(),
            base_decimals: 9,
            quote_decimals: 9,
            coin_reserve: Some(10_000_000_000_000), // 10K tokens
            pc_reserve: Some(1_000_000_000_000),    // 1K SOL
            market_id: Pubkey::new_unique(),
            serum_bids: None,
            serum_asks: None,
            serum_event_queue: None,
            serum_base_vault: None,
            serum_quote_vault: None,
        };

        let intent = create_test_intent(token_mint, wsol_mint, 10_000_000_000); // 10 tokens
        let amount_out =
            super::calculate_raydium_amm_quote(&state, 10_000_000_000, &intent, false).unwrap();

        // 10 tokens in 10K token pool → should get ~1 SOL (with fee and price impact)
        assert!(amount_out > 900_000_000);
        assert!(amount_out < 1_100_000_000);
    }

    #[test]
    fn test_raydium_cpmm_quote() {
        let token_0 = Pubkey::new_unique();
        let token_1 = Pubkey::new_unique();

        let state = RaydiumCpmmState {
            token_0_mint: token_0,
            token_1_mint: token_1,
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: Some(1_000_000_000_000), // 1000 of token_0
            reserve_1: Some(2_000_000_000_000), // 2000 of token_1
        };

        let intent = create_test_intent(token_0, token_1, 10_000_000_000); // 10 token_0
        let amount_out =
            super::calculate_raydium_cpmm_quote(&state, 10_000_000_000, &intent, true).unwrap();

        // 10 of token_0 (1% of pool) → ~20 token_1 (minus fee and price impact)
        assert!(amount_out > 19_000_000_000);
        assert!(amount_out < 21_000_000_000);
    }

    #[test]
    fn test_orca_whirlpool_quote() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();

        let state = OrcaWhirlpoolState {
            token_mint_a: mint_a,
            token_mint_b: mint_b,
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            tick_current_index: 0,
            sqrt_price: 1_000_000_000_000, // Roughly 1:1
            liquidity: 10_000_000_000_000,
            fee_rate: 3000, // 0.3% (in hundredths of bps)
            protocol_fee_rate: 300,
            tick_spacing: 64,
            vault_a_balance: Some(1_000_000_000_000), // 1000 tokens
            vault_b_balance: Some(1_000_000_000_000), // 1000 tokens
            token_a_program: None,
            token_b_program: None,
            whirlpool_quote_account_seeded: true,
        };

        let intent = create_test_intent(mint_a, mint_b, 10_000_000_000); // 10 token_a
        let amount_out =
            super::calculate_orca_quote(&state, 10_000_000_000, &intent, true).unwrap();

        // 10 of token_a → ~10 token_b (minus 0.3% fee and price impact)
        assert!(amount_out > 9_500_000_000);
        assert!(amount_out < 10_500_000_000);
    }

    #[test]
    fn test_meteora_dlmm_quote() {
        let token_x = Pubkey::new_unique();
        let token_y = Pubkey::new_unique();

        let state = MeteoraState {
            token_x_mint: token_x,
            token_y_mint: token_y,
            reserve_x: Pubkey::new_unique(),
            reserve_y: Pubkey::new_unique(),
            active_id: 8388608,                          // Neutral price
            bin_step: 20,                                // 0.2% per bin
            reserve_x_balance: Some(5_000_000_000_000),  // 5000 tokens
            reserve_y_balance: Some(10_000_000_000_000), // 10000 tokens
            dlmm_bin_params_account_seeded: true,
        };

        let intent = create_test_intent(token_x, token_y, 50_000_000_000); // 50 token_x
        let amount_out =
            super::calculate_meteora_quote(&state, 50_000_000_000, &intent, true).unwrap();

        // 50 of token_x (1% of pool) → ~100 token_y (2:1 ratio, minus fee)
        assert!(amount_out > 95_000_000_000);
        assert!(amount_out < 105_000_000_000);
    }

    #[test]
    fn test_meteora_cpmm_quote() {
        let token_0 = Pubkey::new_unique();
        let token_1 = Pubkey::new_unique();

        let state = MeteoraCpmmState {
            token_0_mint: token_0,
            token_1_mint: token_1,
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            amm_config: Pubkey::new_unique(),
            observation_key: Pubkey::new_unique(),
            token_0_program: Pubkey::new_unique(), // Token program pubkey
            token_1_program: Pubkey::new_unique(), // Token program pubkey
            reserve_0: 1_000_000_000_000,          // 1000 of token_0
            reserve_1: 2_000_000_000_000,          // 2000 of token_1
            mint_0_decimals: 9,
            mint_1_decimals: 9,
            status: 0,
        };

        let intent = create_test_intent(token_0, token_1, 10_000_000_000); // 10 token_0
        let amount_out =
            super::calculate_meteora_cpmm_quote(&state, 10_000_000_000, &intent, true).unwrap();

        // 10 of token_0 → ~20 token_1 (2:1 ratio, minus 0.25% fee)
        assert!(amount_out > 19_000_000_000);
        assert!(amount_out < 21_000_000_000);
    }

    #[test]
    fn test_pump_amm_quote_buy() {
        let base_mint = Pubkey::new_unique(); // Token
        let quote_mint = Pubkey::new_unique(); // WSOL

        let state = PumpAmmState {
            base_mint,
            quote_mint,
            pool_base_token_account: Pubkey::new_unique(),
            pool_quote_token_account: Pubkey::new_unique(),
            base_reserve: Some(1_000_000_000_000_000), // 1M tokens
            quote_reserve: Some(50_000_000_000),       // 50 SOL
            pool_accounts: vec![],
            creator: None,
        };

        // Buy tokens with 1 SOL
        let intent = create_test_intent(quote_mint, base_mint, 1_000_000_000);
        let amount_out =
            super::calculate_pumpamm_quote(&state, 1_000_000_000, &intent, true).unwrap();

        // 1 SOL in 50 SOL pool → ~2% of tokens (minus 1% fee and price impact)
        assert!(amount_out > 15_000_000_000_000);
        assert!(amount_out < 25_000_000_000_000);
    }

    #[test]
    fn test_pump_amm_quote_sell() {
        let base_mint = Pubkey::new_unique(); // Token
        let quote_mint = Pubkey::new_unique(); // WSOL

        let state = PumpAmmState {
            base_mint,
            quote_mint,
            pool_base_token_account: Pubkey::new_unique(),
            pool_quote_token_account: Pubkey::new_unique(),
            base_reserve: Some(1_000_000_000_000_000), // 1M tokens
            quote_reserve: Some(50_000_000_000),       // 50 SOL
            pool_accounts: vec![],
            creator: None,
        };

        // Sell 20K tokens
        let intent = create_test_intent(base_mint, quote_mint, 20_000_000_000_000);
        let amount_out =
            super::calculate_pumpamm_quote(&state, 20_000_000_000_000, &intent, false).unwrap();

        // 20K tokens (2% of pool) → ~1 SOL (minus 1% fee and price impact)
        assert!(amount_out > 900_000_000);
        assert!(amount_out < 1_100_000_000);
    }

    #[test]
    fn test_zero_reserves_error() {
        let state = RaydiumCpmmState {
            token_0_mint: Pubkey::new_unique(),
            token_1_mint: Pubkey::new_unique(),
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: Some(0), // Zero reserve!
            reserve_1: Some(1_000_000),
        };

        let intent = create_test_intent(state.token_0_mint, state.token_1_mint, 1_000_000);
        let result = super::calculate_raydium_cpmm_quote(&state, 1_000_000, &intent, true);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing reserves"));
    }

    #[test]
    fn test_pumpfun_complete_bonding_curve_error() {
        let state = PumpFunState {
            token_mint: Pubkey::new_unique(),
            bonding_curve: Pubkey::new_unique(),
            associated_bonding_curve: Pubkey::new_unique(),
            virtual_sol_reserves: 30_000_000_000,
            virtual_token_reserves: 1_000_000_000_000_000,
            real_sol_reserves: 0,
            real_token_reserves: 793_100_000_000_000,
            complete: true, // Migrated!
            creator: Pubkey::new_unique(),
            cashback_enabled: false,
        };

        let result = super::calculate_pumpfun_quote(&state, 1_000_000_000, true);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("complete"));
    }

    #[test]
    fn test_slippage_edge_cases() {
        // Minimum slippage (1 bps = 0.01%)
        assert_eq!(apply_slippage(10000, 1), 9999);

        // Large amount
        assert_eq!(apply_slippage(1_000_000_000_000, 500), 950_000_000_000);

        // Very small amount
        assert_eq!(apply_slippage(100, 500), 95);

        // 99% slippage (9900 bps)
        assert_eq!(apply_slippage(1000, 9900), 10);

        // Overflow protection
        let large = u64::MAX / 2;
        let result = apply_slippage(large, 500);
        assert!(result < large);
    }

    // Helper to create test intents
    fn create_test_intent(input_mint: Pubkey, output_mint: Pubkey, amount: u64) -> TradeIntent {
        use crate::ipc::{
            ExplicitAmount, IntentOrigin, IntentTier, RecordHeader, TradeResources, TradeSide,
            TradingRegime,
        };

        TradeIntent {
            header: RecordHeader::new("test", "0.0.1-test", "test-run-id"),
            intent_id: "test-intent".to_string(),
            source: "test".to_string(),
            tier: IntentTier::Tier1,
            origin_type: IntentOrigin::StrategyA,
            deadline_slot: None,
            ttl_ms: Some(3000),
            side: TradeSide::Buy,
            regime: TradingRegime::Established,
            trigger_event_id: None,
            require_bundle: None,
            bundle_tip_lamports: None,
            hint_compute_units: None,
            hint_priority_fee_micro_lamports: None,
            hint_urgency: None,
            metadata: Default::default(),
            execution: None,
            swap_path: None,
            resources: TradeResources {
                input_mint: input_mint.to_string(),
                output_mint: output_mint.to_string(),
                pools: vec![Pubkey::new_unique().to_string()],
                accounts: vec![],
                token_program: None,
            },
            required_capital: ExplicitAmount {
                raw: amount,
                decimals: 9,
                ui: None,
            },
            expected_roi_bps: 100,
            max_slippage_bps: 500, // 5%
        }
    }
}
