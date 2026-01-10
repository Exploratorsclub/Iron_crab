//! Meteora DLMM Bin Array account layout parser
//!
//! Bin Arrays store the actual liquidity bins for DLMM pools.
//! Each bin array contains multiple bins (typically 70 bins per array).

use anyhow::{ensure, Result};
use solana_sdk::pubkey::Pubkey;

/// Single bin within a bin array
#[derive(Debug, Clone)]
pub struct Bin {
    /// Amount of token X in this bin
    pub amount_x: u64,
    /// Amount of token Y in this bin
    pub amount_y: u64,
    /// Price of this bin (derived from bin_step and bin_id)
    pub price: f64,
}

/// Bin Array account structure
/// Each array contains a contiguous range of bins
#[derive(Debug, Clone)]
pub struct BinArray {
    /// Discriminator (8 bytes)
    pub discriminator: [u8; 8],

    /// Index of this bin array
    /// Determines which range of bins this array covers
    pub index: i64,

    /// Version (1 byte)
    pub version: u8,

    /// Padding (7 bytes)
    pub _padding: [u8; 7],

    /// LB Pair this bin array belongs to
    pub lb_pair: Pubkey,

    /// Individual bins (70 bins per array in Meteora DLMM)
    pub bins: Vec<Bin>,
}

impl BinArray {
    /// Number of bins per array (Meteora DLMM standard)
    pub const BINS_PER_ARRAY: usize = 70;

    /// Expected account data size for bin array
    /// 8 (discriminator) + 8 (index) + 1 (version) + 7 (padding) + 32 (lb_pair) + (70 * 24) (bins)
    /// = 1736 bytes
    pub const ACCOUNT_SIZE: usize = 8 + 8 + 1 + 7 + 32 + (Self::BINS_PER_ARRAY * 24);

    /// Parse bin array from raw account data
    pub fn parse(data: &[u8], bin_step: u16) -> Result<Self> {
        ensure!(
            data.len() >= 56, // Minimum: header without bins
            "Invalid bin array size: {} (expected at least 56)",
            data.len()
        );

        // Parse discriminator (first 8 bytes)
        let discriminator: [u8; 8] = data[0..8].try_into()?;

        // Parse index (offset 8, 8 bytes, i64 LE)
        let index = i64::from_le_bytes(data[8..16].try_into()?);

        // Parse version (offset 16, 1 byte)
        let version = data[16];

        // Parse padding (offset 17, 7 bytes)
        let _padding: [u8; 7] = data[17..24].try_into()?;

        // Parse lb_pair (offset 24, 32 bytes)
        let lb_pair = Pubkey::new_from_array(data[24..56].try_into()?);

        // Parse bins (starting at offset 56)
        // Each bin: amount_x (u128, 16 bytes) + amount_y (u128, 16 bytes) = 32 bytes
        // But we use u64 for practical purposes (upper 64 bits typically zero)
        let mut bins = Vec::with_capacity(Self::BINS_PER_ARRAY);

        let bins_data_start = 56;
        for i in 0..Self::BINS_PER_ARRAY {
            let bin_offset = bins_data_start + (i * 32); // Each bin is 32 bytes

            if bin_offset + 32 > data.len() {
                break; // Not enough data for this bin
            }

            // Amount X: u128 LE (we use lower 64 bits)
            let amount_x = u64::from_le_bytes(data[bin_offset..bin_offset + 8].try_into()?);

            // Amount Y: u128 LE (we use lower 64 bits)
            let amount_y = u64::from_le_bytes(data[bin_offset + 16..bin_offset + 24].try_into()?);

            // Calculate bin ID from array index
            let bin_id = (index * Self::BINS_PER_ARRAY as i64) + i as i64;

            // Calculate price from bin_id and bin_step
            // Price formula: (1 + bin_step/10000)^bin_id
            let price = Self::calculate_price(bin_id as i32, bin_step);

            bins.push(Bin {
                amount_x,
                amount_y,
                price,
            });
        }

        Ok(Self {
            discriminator,
            index,
            version,
            _padding,
            lb_pair,
            bins,
        })
    }

    /// Calculate price for a given bin ID and bin step
    fn calculate_price(bin_id: i32, bin_step: u16) -> f64 {
        let base = 1.0 + (bin_step as f64 / 10000.0);
        base.powi(bin_id)
    }

    /// Get bin by offset within this array (0-69)
    pub fn get_bin(&self, offset: usize) -> Option<&Bin> {
        self.bins.get(offset)
    }

    /// Get the global bin ID for a given offset in this array
    pub fn offset_to_bin_id(&self, offset: usize) -> i64 {
        (self.index * Self::BINS_PER_ARRAY as i64) + offset as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_calculation() {
        // bin_step = 1 (0.01%), bin_id = 0
        let price_0 = BinArray::calculate_price(0, 1);
        assert_eq!(price_0, 1.0);

        // bin_step = 100 (1%), bin_id = 100
        let price_100 = BinArray::calculate_price(100, 100);
        assert!(price_100 > 2.7); // ~e^1 = 2.718

        // Negative bin_id
        let price_neg = BinArray::calculate_price(-100, 100);
        assert!(price_neg < 1.0);
    }

    #[test]
    fn test_offset_to_bin_id() {
        let bin_array = BinArray {
            discriminator: [0; 8],
            index: 5,
            version: 0,
            _padding: [0; 7],
            lb_pair: Pubkey::default(),
            bins: Vec::new(),
        };

        // Array index 5, offset 0 -> bin_id = 5 * 70 + 0 = 350
        assert_eq!(bin_array.offset_to_bin_id(0), 350);

        // Array index 5, offset 69 -> bin_id = 5 * 70 + 69 = 419
        assert_eq!(bin_array.offset_to_bin_id(69), 419);
    }
}
