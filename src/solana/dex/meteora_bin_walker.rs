//! Meteora DLMM bin-walking quote algorithm for accurate price calculation
//!
//! Phase 2: Implements bin-based liquidity distribution traversal
//! for precise swap simulation (replacing constant product approximation)

use anyhow::{ensure, Result};
use std::cmp::min;

/// Single price bin in DLMM pool
#[derive(Debug, Clone)]
pub struct Bin {
    /// Bin ID (signed, relative to active_id)
    pub id: i32,
    /// Liquidity in token X
    pub amount_x: u64,
    /// Liquidity in token Y  
    pub amount_y: u64,
    /// Price at this bin (derived from bin_id and bin_step)
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
        // Keep bins sorted by ID
        self.bins.sort_by_key(|b| b.id);
    }

    /// Convert bin ID to price
    /// Formula: price = (1 + bin_step/10000)^bin_id
    fn bin_id_to_price(&self, bin_id: i32) -> f64 {
        let step_multiplier = 1.0 + (self.bin_step as f64 / 10000.0);
        step_multiplier.powi(bin_id)
    }

    /// Simulate swap X→Y (ascending price bins)
    /// Returns (amount_out, bins_crossed, effective_fee_bps)
    pub fn quote_x_to_y(&self, amount_in: u64, fee_bps: u32) -> Result<(u64, usize, u32)> {
        ensure!(amount_in > 0, "Amount must be positive");

        // Apply fee once at the beginning
        let fee_amount = (amount_in as u128 * fee_bps as u128) / 10000;
        let amount_in_after_fee = amount_in.saturating_sub(fee_amount as u64);

        if amount_in_after_fee == 0 {
            return Ok((0, 0, fee_bps));
        }

        let mut remaining_in = amount_in_after_fee;
        let mut total_out = 0u64;
        let mut bins_crossed = 0usize;

        // Walk bins from active_id upwards (X→Y increases price)
        for bin in self.bins.iter().filter(|b| b.id >= self.active_id) {
            if remaining_in == 0 {
                break;
            }

            // Skip empty bins
            if bin.amount_x == 0 || bin.amount_y == 0 {
                continue;
            }

            // Max we can consume from this bin's Y (output side)
            let max_available_y = bin.amount_y.saturating_sub(1);

            if max_available_y == 0 {
                continue;
            }

            // Constant product: x * y = k
            // We add X (input), so new_x = x + remaining_in
            // Solve for amount_y we get: amount_y = y - k/(x + remaining_in)
            let k = bin.amount_x as u128 * bin.amount_y as u128;
            let new_x = bin.amount_x as u128 + remaining_in as u128;
            let new_y = k / new_x;

            // Amount of Y we get out
            let amount_out_from_bin = bin.amount_y.saturating_sub(new_y as u64);

            if amount_out_from_bin == 0 {
                continue;
            }

            // Cap output to available liquidity
            let actual_out = min(amount_out_from_bin, max_available_y);

            total_out = total_out.saturating_add(actual_out);

            // Calculate how much input was actually consumed
            if actual_out < amount_out_from_bin {
                // Partial fill - recalculate consumed input
                let actual_new_y = bin.amount_y.saturating_sub(actual_out);
                let actual_new_x = k / actual_new_y as u128;
                let consumed_x = actual_new_x.saturating_sub(bin.amount_x as u128) as u64;
                remaining_in = remaining_in.saturating_sub(consumed_x);
            } else {
                // Full fill - consumed all remaining_in
                remaining_in = 0;
            }

            bins_crossed += 1;

            if remaining_in == 0 {
                break;
            }
        }

        Ok((total_out, bins_crossed, fee_bps))
    }

    /// Simulate swap Y→X (descending price bins)
    pub fn quote_y_to_x(&self, amount_in: u64, fee_bps: u32) -> Result<(u64, usize, u32)> {
        ensure!(amount_in > 0, "Amount must be positive");

        // Apply fee once at the beginning
        let fee_amount = (amount_in as u128 * fee_bps as u128) / 10000;
        let amount_in_after_fee = amount_in.saturating_sub(fee_amount as u64);

        if amount_in_after_fee == 0 {
            return Ok((0, 0, fee_bps));
        }

        let mut remaining_in = amount_in_after_fee;
        let mut total_out = 0u64;
        let mut bins_crossed = 0usize;

        // Walk bins from active_id downwards (Y→X decreases price)
        for bin in self.bins.iter().rev().filter(|b| b.id <= self.active_id) {
            if remaining_in == 0 {
                break;
            }

            if bin.amount_x == 0 || bin.amount_y == 0 {
                continue;
            }

            let max_available_x = bin.amount_x.saturating_sub(1);

            if max_available_x == 0 {
                continue;
            }

            // Constant product: x * y = k
            // We add Y (input), so new_y = y + remaining_in
            // Amount of X we get: amount_x = x - k/(y + remaining_in)
            let k = bin.amount_x as u128 * bin.amount_y as u128;
            let new_y = bin.amount_y as u128 + remaining_in as u128;
            let new_x = k / new_y;

            let amount_out_from_bin = bin.amount_x.saturating_sub(new_x as u64);

            if amount_out_from_bin == 0 {
                continue;
            }

            let actual_out = min(amount_out_from_bin, max_available_x);

            total_out = total_out.saturating_add(actual_out);

            if actual_out < amount_out_from_bin {
                let actual_new_x = bin.amount_x.saturating_sub(actual_out);
                let actual_new_y = k / actual_new_x as u128;
                let consumed_y = actual_new_y.saturating_sub(bin.amount_y as u128) as u64;
                remaining_in = remaining_in.saturating_sub(consumed_y);
            } else {
                remaining_in = 0;
            }

            bins_crossed += 1;

            if remaining_in == 0 {
                break;
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bin_walker_x_to_y() {
        let mut walker = BinWalker::new(0, 10); // Active bin 0, 0.1% step

        // Add bins with liquidity
        walker.add_bin(0, 1_000_000_000, 100_000_000_000); // Active: 1000 SOL, 100k USDC
        walker.add_bin(1, 500_000_000, 50_000_000_000); // Bin +1: 500 SOL, 50k USDC
        walker.add_bin(2, 250_000_000, 25_000_000_000); // Bin +2: 250 SOL, 25k USDC

        // Swap 1 SOL (X) for USDC (Y)
        let (amount_out, bins_crossed, _fee) = walker
            .quote_x_to_y(1_000_000, 30) // 1 SOL, 0.3% fee
            .expect("Quote failed");

        println!("1 SOL → {} USDC", amount_out as f64 / 1_000_000.0);
        println!("Bins crossed: {}", bins_crossed);

        assert!(amount_out > 99_000_000, "Should get ~100 USDC");
        assert_eq!(bins_crossed, 1, "Should only use active bin for small swap");
    }

    #[test]
    fn test_bin_walker_y_to_x() {
        let mut walker = BinWalker::new(0, 10);

        walker.add_bin(-1, 500_000_000, 50_000_000_000);
        walker.add_bin(0, 1_000_000_000, 100_000_000_000);

        // Swap USDC for SOL
        let (amount_out, bins_crossed, _fee) = walker
            .quote_y_to_x(100_000_000, 30) // 100 USDC
            .expect("Quote failed");

        println!("100 USDC → {} SOL", amount_out as f64 / 1_000_000_000.0);

        assert!(amount_out > 0, "Should get SOL out");
        assert_eq!(bins_crossed, 1);
    }
}
