//! Meteora DLMM (Dynamic Liquidity Market Maker) pool account layout parser
//!
//! LB Pair account structure reverse-engineered from mainnet data.
//! Reference pool: 5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 (WSOL-USDC)

use anyhow::{ensure, Result};
use solana_sdk::pubkey::Pubkey;

/// Meteora DLMM LB Pair account (904 bytes)
#[derive(Debug, Clone)]
pub struct DlmmPool {
    /// Discriminator (8 bytes) - Anchor discriminator
    pub discriminator: [u8; 8],
    
    /// Bin step - price step between bins (basis points)
    /// At offset 0x20 (32 decimal)
    pub bin_step: u16,
    
    /// Active bin ID - current price bin
    /// At offset 0x30 (48 decimal) - signed i32
    pub active_id: i32,
    
    /// Token X mint (32 bytes)
    /// At offset 0x58 (88 decimal)
    pub token_x_mint: Pubkey,
    
    /// Token Y mint (32 bytes)
    /// At offset 0x78 (120 decimal)
    pub token_y_mint: Pubkey,
    
    /// Reserve X vault (32 bytes)
    /// At offset 0x98 (152 decimal)
    pub reserve_x: Pubkey,
    
    /// Reserve Y vault (32 bytes)
    /// At offset 0xB8 (184 decimal)
    pub reserve_y: Pubkey,
}

impl DlmmPool {
    /// Expected account data size for LB Pair
    pub const ACCOUNT_SIZE: usize = 904;
    
    /// Parse LB Pair account from raw account data
    pub fn parse(data: &[u8]) -> Result<Self> {
        ensure!(
            data.len() >= Self::ACCOUNT_SIZE,
            "Invalid account size: {} (expected {})",
            data.len(),
            Self::ACCOUNT_SIZE
        );
        
        // Discriminator (8 bytes)
        let discriminator = data[0..8].try_into().unwrap();
        
        // Bin step at offset 0x20 (32) - u16 little endian
        let bin_step = u16::from_le_bytes(data[0x20..0x22].try_into().unwrap());
        
        // Active ID at offset 0x30 (48) - i32 little endian
        let active_id = i32::from_le_bytes(data[0x30..0x34].try_into().unwrap());
        
        // Token X mint at offset 0x58 (88)
        let token_x_bytes: [u8; 32] = data[0x58..0x78].try_into().unwrap();
        let token_x_mint = Pubkey::new_from_array(token_x_bytes);
        
        // Token Y mint at offset 0x78 (120)
        let token_y_bytes: [u8; 32] = data[0x78..0x98].try_into().unwrap();
        let token_y_mint = Pubkey::new_from_array(token_y_bytes);
        
        // Reserve X at offset 0x98 (152)
        let reserve_x_bytes: [u8; 32] = data[0x98..0xB8].try_into().unwrap();
        let reserve_x = Pubkey::new_from_array(reserve_x_bytes);
        
        // Reserve Y at offset 0xB8 (184)
        let reserve_y_bytes: [u8; 32] = data[0xB8..0xD8].try_into().unwrap();
        let reserve_y = Pubkey::new_from_array(reserve_y_bytes);
        
        Ok(Self {
            discriminator,
            bin_step,
            active_id,
            token_x_mint,
            token_y_mint,
            reserve_x,
            reserve_y,
        })
    }
    
    /// Get token pair as strings for logging
    pub fn token_pair(&self) -> (String, String) {
        (self.token_x_mint.to_string(), self.token_y_mint.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_wsol_usdc_pool() {
        // Real WSOL-USDC pool: 5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6
        let data = include_bytes!("../../../meteora_lb_pair_real.bin");
        
        let pool = DlmmPool::parse(data).expect("Failed to parse pool");
        
        // Verify token mints
        assert_eq!(
            pool.token_x_mint.to_string(),
            "So11111111111111111111111111111111111111112",
            "Token X should be WSOL"
        );
        
        assert_eq!(
            pool.token_y_mint.to_string(),
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "Token Y should be USDC"
        );
        
        // Verify bin_step is reasonable (typically 1-1000 bps)
        assert!(pool.bin_step > 0 && pool.bin_step < 10000, "bin_step should be 1-10000");
        
        // Verify active_id is set (non-zero for active pool)
        assert_ne!(pool.active_id, 0, "active_id should be non-zero");
        
        // Verify reserve pubkeys are valid (not all zeros)
        assert_ne!(pool.reserve_x, Pubkey::default(), "reserve_x should not be default");
        assert_ne!(pool.reserve_y, Pubkey::default(), "reserve_y should not be default");
        
        println!("✅ Parsed WSOL-USDC DLMM Pool:");
        println!("   Token X: {}", pool.token_x_mint);
        println!("   Token Y: {}", pool.token_y_mint);
        println!("   Bin Step: {} bps", pool.bin_step);
        println!("   Active ID: {}", pool.active_id);
        println!("   Reserve X: {}", pool.reserve_x);
        println!("   Reserve Y: {}", pool.reserve_y);
    }
}
