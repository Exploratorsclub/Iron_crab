//! Meteora DLMM bin-walking quote algorithm for accurate price calculation
//!
//! Program-near constant-price bin walk (Meteora DLMM), not per-bin xy=k CPMM.

use anyhow::{ensure, Result};
use std::cmp::min;
use std::collections::HashMap;

const BASIS_POINT_MAX: u128 = 10_000;
const SCALE_OFFSET: u8 = 64;
/// `1.0` in Q64.64.
const ONE: u128 = 1u128 << SCALE_OFFSET;
/// Above this exponent the result overflows the Q64.64 range (Meteora on-chain bound).
const MAX_EXPONENTIAL: u32 = 0x80000;

/// Single price bin in DLMM pool
#[derive(Debug, Clone)]
pub struct Bin {
    /// Bin ID (signed, relative to active_id)
    pub id: i32,
    /// Liquidity in token X
    pub amount_x: u64,
    /// Liquidity in token Y
    pub amount_y: u64,
    /// Spot price at this bin (diagnostics / price impact only — not used for fill math)
    pub price: f64,
}

/// Bin-walking quote calculation
pub struct BinWalker {
    /// Current bin ID (active price)
    pub active_id: i32,
    /// Bin step in basis points
    pub bin_step: u16,
    /// All bins with liquidity (sorted by ID)
    pub bins: Vec<Bin>,
}

impl BinWalker {
    /// Create bin walker from pool state
    pub fn new(active_id: i32, bin_step: u16) -> Self {
        Self {
            active_id,
            bin_step,
            bins: Vec::new(),
        }
    }

    /// Add bin to the walker (maintains sorted order)
    pub fn add_bin(&mut self, id: i32, amount_x: u64, amount_y: u64) {
        let price = self.bin_id_to_price(id);
        self.bins.push(Bin {
            id,
            amount_x,
            amount_y,
            price,
        });
        self.bins.sort_by_key(|b| b.id);
    }

    /// Convert bin ID to price (diagnostics only).
    /// Formula: price = (1 + bin_step/10000)^bin_id
    pub fn bin_id_to_price(&self, bin_id: i32) -> f64 {
        let step_multiplier = 1.0 + (self.bin_step as f64 / 10000.0);
        step_multiplier.powi(bin_id)
    }

    /// Simulate swap X→Y (`swap_for_y = true`, walk downward).
    /// Returns (amount_out, bins_crossed, effective_fee_bps)
    pub fn quote_x_to_y(&self, amount_in: u64, fee_bps: u32) -> Result<(u64, usize, u32)> {
        self.quote_exact_in(amount_in, fee_bps, true)
    }

    /// Simulate swap Y→X (`swap_for_y = false`, walk upward).
    pub fn quote_y_to_x(&self, amount_in: u64, fee_bps: u32) -> Result<(u64, usize, u32)> {
        self.quote_exact_in(amount_in, fee_bps, false)
    }

    fn quote_exact_in(
        &self,
        amount_in: u64,
        fee_bps: u32,
        swap_for_y: bool,
    ) -> Result<(u64, usize, u32)> {
        ensure!(amount_in > 0, "Amount must be positive");

        let fee_amount = (amount_in as u128 * fee_bps as u128) / 10_000;
        let amount_in_after_fee = amount_in.saturating_sub(fee_amount as u64);

        if amount_in_after_fee == 0 {
            return Ok((0, 0, fee_bps));
        }

        if self.bins.is_empty() {
            return Ok((0, 0, fee_bps));
        }

        let min_id = self.bins.first().map(|b| b.id).unwrap_or(self.active_id);
        let max_id = self.bins.last().map(|b| b.id).unwrap_or(self.active_id);
        let step: i32 = if swap_for_y { -1 } else { 1 };

        let bin_map: HashMap<i32, (u64, u64)> = self
            .bins
            .iter()
            .map(|b| (b.id, (b.amount_x, b.amount_y)))
            .collect();

        let mut remaining_in = amount_in_after_fee;
        let mut total_out = 0u64;
        let mut bins_crossed = 0usize;
        let mut current_id = self.active_id;

        while remaining_in > 0 {
            if swap_for_y && current_id < min_id {
                break;
            }
            if !swap_for_y && current_id > max_id {
                break;
            }

            if let Some(&(amount_x, amount_y)) = bin_map.get(&current_id) {
                let max_out = if swap_for_y { amount_y } else { amount_x };
                if max_out > 0 {
                    let price = get_price_from_id(current_id, self.bin_step).ok_or_else(|| {
                        anyhow::anyhow!("DLMM price overflow for bin {}", current_id)
                    })?;

                    let ideal_out = get_amount_out(remaining_in, price, swap_for_y)?;
                    let actual_out = min(ideal_out, max_out);

                    if actual_out > 0 {
                        total_out = total_out.saturating_add(actual_out);
                        let consumed_in = if actual_out == ideal_out {
                            remaining_in
                        } else {
                            get_amount_in(actual_out, price, swap_for_y)?
                        };
                        remaining_in = remaining_in.saturating_sub(consumed_in);
                        bins_crossed += 1;
                    }
                }
            }

            if remaining_in == 0 {
                break;
            }
            current_id += step;
        }

        Ok((total_out, bins_crossed, fee_bps))
    }

    /// Calculate price impact in basis points
    pub fn calculate_price_impact(&self, amount_in: u64, amount_out: u64, is_x_to_y: bool) -> u32 {
        if amount_in == 0 || amount_out == 0 {
            return 0;
        }

        let spot_price = self.bin_id_to_price(self.active_id);
        let effective_price = if is_x_to_y {
            amount_out as f64 / amount_in as f64
        } else {
            amount_in as f64 / amount_out as f64
        };

        ((effective_price - spot_price).abs() / spot_price * 10000.0) as u32
    }
}

/// Q64.64 bin price: `(1 + bin_step/10_000)^bin_id`.
fn get_price_from_id(active_id: i32, bin_step: u16) -> Option<u128> {
    let bps = (bin_step as u128).checked_shl(SCALE_OFFSET.into())? / BASIS_POINT_MAX;
    let base = ONE.checked_add(bps)?;
    pow(base, active_id)
}

/// `base^exp` in Q64.64 (ported from Meteora on-chain / solana-protocols).
fn pow(base: u128, exp: i32) -> Option<u128> {
    let mut invert = exp.is_negative();

    if exp == 0 {
        return Some(ONE);
    }

    let exp: u32 = if invert {
        exp.unsigned_abs()
    } else {
        exp as u32
    };

    if exp >= MAX_EXPONENTIAL {
        return None;
    }

    let mut squared_base = base;
    let mut result = ONE;

    if squared_base >= result {
        squared_base = u128::MAX.checked_div(squared_base)?;
        invert = !invert;
    }

    macro_rules! pow_step {
        ($mask:expr) => {
            if exp & $mask > 0 {
                result = (result.checked_mul(squared_base)?) >> SCALE_OFFSET;
            }
            squared_base = (squared_base.checked_mul(squared_base)?) >> SCALE_OFFSET;
        };
    }

    pow_step!(0x1);
    pow_step!(0x2);
    pow_step!(0x4);
    pow_step!(0x8);
    pow_step!(0x10);
    pow_step!(0x20);
    pow_step!(0x40);
    pow_step!(0x80);
    pow_step!(0x100);
    pow_step!(0x200);
    pow_step!(0x400);
    pow_step!(0x800);
    pow_step!(0x1000);
    pow_step!(0x2000);
    pow_step!(0x4000);
    pow_step!(0x8000);
    pow_step!(0x10000);
    pow_step!(0x20000);
    if exp & 0x40000 > 0 {
        result = (result.checked_mul(squared_base)?) >> SCALE_OFFSET;
    }

    if result == 0 {
        return None;
    }

    if invert {
        result = u128::MAX.checked_div(result)?;
    }

    Some(result)
}

/// `floor(a * b / 2^64)`
fn mul_shr(a: u128, b: u128) -> Option<u128> {
    Some((a.checked_mul(b)?) >> SCALE_OFFSET)
}

/// `ceil(a * b / 2^64)`
fn mul_shr_ceil(a: u128, b: u128) -> Option<u128> {
    let product = a.checked_mul(b)?;
    let mask = (1u128 << SCALE_OFFSET) - 1;
    Some((product + mask) >> SCALE_OFFSET)
}

/// `floor(a * 2^64 / b)`
fn shl_div(a: u128, b: u128) -> Option<u128> {
    if b == 0 {
        return None;
    }
    Some(a.checked_shl(SCALE_OFFSET.into())? / b)
}

/// `ceil(a * 2^64 / b)`
fn shl_div_ceil(a: u128, b: u128) -> Option<u128> {
    if b == 0 {
        return None;
    }
    let numerator = a.checked_shl(SCALE_OFFSET.into())?;
    Some(numerator.div_ceil(b))
}

fn get_amount_out(amount_in: u64, price: u128, swap_for_y: bool) -> Result<u64> {
    let out = if swap_for_y {
        mul_shr(amount_in as u128, price)
    } else {
        shl_div(amount_in as u128, price)
    }
    .ok_or_else(|| anyhow::anyhow!("DLMM amount_out overflow"))?;
    u64::try_from(out).map_err(|_| anyhow::anyhow!("DLMM amount_out exceeds u64"))
}

fn get_amount_in(amount_out: u64, price: u128, swap_for_y: bool) -> Result<u64> {
    let inp = if swap_for_y {
        shl_div_ceil(amount_out as u128, price)
    } else {
        mul_shr_ceil(amount_out as u128, price)
    }
    .ok_or_else(|| anyhow::anyhow!("DLMM amount_in overflow"))?;
    u64::try_from(inp).map_err(|_| anyhow::anyhow!("DLMM amount_in exceeds u64"))
}

/// DLMM fee estimate used by arb-strategy marginal probe (matches legacy arb formula).
pub fn dlmm_fee_bps(bin_step: u16) -> u32 {
    10 + (bin_step as u32).min(100)
}

/// Build a walker from flattened bin liquidity `(bin_id, amount_x, amount_y)`.
pub fn walker_from_bins(active_id: i32, bin_step: u16, bins: &[(i32, u64, u64)]) -> BinWalker {
    let mut walker = BinWalker::new(active_id, bin_step);
    for &(id, amount_x, amount_y) in bins {
        if amount_x > 0 || amount_y > 0 {
            walker.add_bin(id, amount_x, amount_y);
        }
    }
    walker
}

#[cfg(test)]
mod tests {
    use super::*;

    fn in_after_fee(amount_in: u64, fee_bps: u32) -> u64 {
        let fee = (amount_in as u128 * fee_bps as u128) / 10_000;
        amount_in.saturating_sub(fee as u64)
    }

    #[test]
    fn test_bin_walker_x_to_y_constant_price_active_bin() {
        let mut walker = BinWalker::new(0, 10);
        walker.add_bin(0, 1_000_000_000, 100_000_000_000);
        walker.add_bin(1, 500_000_000, 50_000_000_000);
        walker.add_bin(2, 250_000_000, 25_000_000_000);

        let amount_in = 1_000_000u64;
        let fee_bps = 30u32;
        let (amount_out, bins_crossed, _) = walker.quote_x_to_y(amount_in, fee_bps).unwrap();

        let price = get_price_from_id(0, 10).unwrap();
        let expected = mul_shr(in_after_fee(amount_in, fee_bps) as u128, price).unwrap() as u64;

        assert_eq!(amount_out, expected);
        assert_eq!(bins_crossed, 1);
    }

    #[test]
    fn test_bin_walker_y_to_x_constant_price_active_bin() {
        let mut walker = BinWalker::new(0, 10);
        walker.add_bin(-1, 500_000_000, 50_000_000_000);
        walker.add_bin(0, 1_000_000_000, 100_000_000_000);

        let amount_in = 100_000_000u64;
        let fee_bps = 30u32;
        let (amount_out, bins_crossed, _) = walker.quote_y_to_x(amount_in, fee_bps).unwrap();

        let price = get_price_from_id(0, 10).unwrap();
        let expected = shl_div(in_after_fee(amount_in, fee_bps) as u128, price).unwrap() as u64;

        assert_eq!(amount_out, expected);
        assert_eq!(bins_crossed, 1);
    }

    #[test]
    fn test_one_sided_bin_x_to_y_produces_output() {
        let mut walker = BinWalker::new(0, 10);
        walker.add_bin(0, 0, 100_000_000);

        let (amount_out, bins_crossed, _) = walker.quote_x_to_y(1_000_000, 30).unwrap();
        assert!(amount_out > 0, "one-sided Y liquidity must quote X→Y");
        assert_eq!(bins_crossed, 1);
    }

    #[test]
    fn test_small_exact_in_matches_integer_formula_not_cpmm() {
        let active_id = 5i32;
        let bin_step = 100u16;
        let mut walker = BinWalker::new(active_id, bin_step);
        walker.add_bin(active_id, 10_000_000_000, 10_000_000_000);

        let amount_in = 50_000u64;
        let fee_bps = 0u32;
        let (amount_out, _, _) = walker.quote_x_to_y(amount_in, fee_bps).unwrap();

        let price = get_price_from_id(active_id, bin_step).unwrap();
        let expected = mul_shr(amount_in as u128, price).unwrap() as u64;
        assert_eq!(amount_out, expected);
        assert!(price > ONE, "non-zero active_id must have price > 1");

        let cpmm_k = 10_000_000_000u128 * 10_000_000_000u128;
        let new_x = 10_000_000_000u128 + amount_in as u128;
        let cpmm_out = 10_000_000_000u64 - (cpmm_k / new_x) as u64;
        assert_ne!(
            amount_out, cpmm_out,
            "constant-price must diverge from xy=k"
        );
    }

    #[test]
    fn test_walk_x_to_y_descends_to_lower_neighbor() {
        let mut walker = BinWalker::new(0, 10);
        walker.add_bin(0, 0, 1_000);
        walker.add_bin(-1, 0, 1_000_000_000);

        let (amount_out, bins_crossed, _) = walker.quote_x_to_y(500_000, 0).unwrap();
        assert!(amount_out > 1_000, "should continue into bin -1");
        assert_eq!(bins_crossed, 2);
    }

    #[test]
    fn test_amount_in_zero_is_error() {
        let walker = BinWalker::new(0, 10);
        assert!(walker.quote_x_to_y(0, 30).is_err());
        assert!(walker.quote_y_to_x(0, 30).is_err());
    }

    #[test]
    fn test_quote_monotonicity_larger_in_not_smaller_out() {
        let mut walker = BinWalker::new(0, 25);
        walker.add_bin(0, 1_000_000_000, 1_000_000_000);
        walker.add_bin(-1, 0, 1_000_000_000);
        walker.add_bin(-2, 0, 1_000_000_000);

        let (small_out, _, _) = walker.quote_x_to_y(100_000, 30).unwrap();
        let (large_out, _, _) = walker.quote_x_to_y(500_000, 30).unwrap();
        assert!(large_out >= small_out);
        assert!(small_out > 0);
    }

    #[test]
    fn get_price_from_id_zero_is_one() {
        assert_eq!(get_price_from_id(0, 10), Some(ONE));
    }

    #[test]
    fn test_get_amount_in_ceil_when_bin_output_capped() {
        let active_id = 7i32;
        let bin_step = 100u16;
        let price = get_price_from_id(active_id, bin_step).unwrap();
        let capped_out = 1_337u64;

        let floor_in = shl_div(capped_out as u128, price).unwrap();
        let ceil_in = get_amount_in(capped_out, price, true).unwrap();
        assert!(
            ceil_in as u128 >= floor_in,
            "ceil consumed input must not be below floor"
        );
        assert!(
            ceil_in as u128 > floor_in,
            "test requires price/amount where ceil > floor"
        );

        let mut walker = BinWalker::new(active_id, bin_step);
        walker.add_bin(active_id, 10_000_000, capped_out);
        walker.add_bin(active_id - 1, 0, 10_000_000_000);

        let amount_in = 500_000u64;
        let (total_out, bins_crossed, _) = walker.quote_x_to_y(amount_in, 0).unwrap();
        assert_eq!(bins_crossed, 2, "active bin exhausted then neighbor");
        assert!(total_out > capped_out);

        let ideal_out_active = get_amount_out(amount_in, price, true).unwrap();
        assert!(
            ideal_out_active > capped_out,
            "input must exceed active bin output cap"
        );
        let consumed_active = get_amount_in(capped_out, price, true).unwrap();
        assert!(
            consumed_active as u128 >= floor_in,
            "partial fill must charge at least ceil(required_in)"
        );
        assert!(amount_in > consumed_active, "must leave input for next bin");
    }
}
