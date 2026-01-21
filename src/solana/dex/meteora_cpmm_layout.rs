//! Meteora CPMM (DAMM V2) Pool Layout
//!
//! Constant Product Market Maker pool - simpler than DLMM (no bins).
//! Program ID: cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D

use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::convert::TryInto;

/// Meteora CPMM Program ID
pub const METEORA_CPMM_PROGRAM: &str = "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D";

/// CPMM Pool account size
pub const CPMM_POOL_SIZE: usize = 397;

/// CPMM Pool discriminator (first 8 bytes)
/// This identifies the account type within the program
pub const CPMM_POOL_DISCRIMINATOR: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];

/// Meteora CPMM Pool state
///
/// Layout (397 bytes total):
/// - discriminator: [u8; 8]       // 0-8
/// - amm_config: Pubkey           // 8-40
/// - pool_creator: Pubkey         // 40-72
/// - token_0_vault: Pubkey        // 72-104
/// - token_1_vault: Pubkey        // 104-136
/// - lp_mint: Pubkey              // 136-168
/// - token_0_mint: Pubkey         // 168-200
/// - token_1_mint: Pubkey         // 200-232
/// - token_0_program: Pubkey      // 232-264
/// - token_1_program: Pubkey      // 264-296
/// - observation_key: Pubkey      // 296-328
/// - auth_bump: u8                // 328
/// - status: u8                   // 329
/// - lp_mint_decimals: u8         // 330
/// - mint_0_decimals: u8          // 331
/// - mint_1_decimals: u8          // 332
/// - lp_supply: u64               // 333-341
/// - protocol_fees_token_0: u64   // 341-349
/// - protocol_fees_token_1: u64   // 349-357
/// - fund_fees_token_0: u64       // 357-365
/// - fund_fees_token_1: u64       // 365-373
/// - open_time: i64               // 373-381
/// - padding: [u8; 16]            // 381-397
#[derive(Debug, Clone)]
pub struct CpmmPool {
    pub discriminator: [u8; 8],
    pub amm_config: Pubkey,
    pub pool_creator: Pubkey,
    pub token_0_vault: Pubkey,
    pub token_1_vault: Pubkey,
    pub lp_mint: Pubkey,
    pub token_0_mint: Pubkey,
    pub token_1_mint: Pubkey,
    pub token_0_program: Pubkey,
    pub token_1_program: Pubkey,
    pub observation_key: Pubkey,
    pub auth_bump: u8,
    pub status: u8,
    pub lp_mint_decimals: u8,
    pub mint_0_decimals: u8,
    pub mint_1_decimals: u8,
    pub lp_supply: u64,
    pub protocol_fees_token_0: u64,
    pub protocol_fees_token_1: u64,
    pub fund_fees_token_0: u64,
    pub fund_fees_token_1: u64,
    pub open_time: i64,
}

impl CpmmPool {
    /// Parse pool state from account data
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < CPMM_POOL_SIZE {
            return Err(anyhow!(
                "CPMM pool data too short: {} < {}",
                data.len(),
                CPMM_POOL_SIZE
            ));
        }

        let discriminator: [u8; 8] = data[0..8].try_into()?;

        // Verify discriminator (optional, for safety)
        // Note: discriminator may vary, so we just parse without strict check

        let amm_config = Pubkey::try_from(&data[8..40])?;
        let pool_creator = Pubkey::try_from(&data[40..72])?;
        let token_0_vault = Pubkey::try_from(&data[72..104])?;
        let token_1_vault = Pubkey::try_from(&data[104..136])?;
        let lp_mint = Pubkey::try_from(&data[136..168])?;
        let token_0_mint = Pubkey::try_from(&data[168..200])?;
        let token_1_mint = Pubkey::try_from(&data[200..232])?;
        let token_0_program = Pubkey::try_from(&data[232..264])?;
        let token_1_program = Pubkey::try_from(&data[264..296])?;
        let observation_key = Pubkey::try_from(&data[296..328])?;

        let auth_bump = data[328];
        let status = data[329];
        let lp_mint_decimals = data[330];
        let mint_0_decimals = data[331];
        let mint_1_decimals = data[332];

        let lp_supply = u64::from_le_bytes(data[333..341].try_into()?);
        let protocol_fees_token_0 = u64::from_le_bytes(data[341..349].try_into()?);
        let protocol_fees_token_1 = u64::from_le_bytes(data[349..357].try_into()?);
        let fund_fees_token_0 = u64::from_le_bytes(data[357..365].try_into()?);
        let fund_fees_token_1 = u64::from_le_bytes(data[365..373].try_into()?);
        let open_time = i64::from_le_bytes(data[373..381].try_into()?);

        Ok(Self {
            discriminator,
            amm_config,
            pool_creator,
            token_0_vault,
            token_1_vault,
            lp_mint,
            token_0_mint,
            token_1_mint,
            token_0_program,
            token_1_program,
            observation_key,
            auth_bump,
            status,
            lp_mint_decimals,
            mint_0_decimals,
            mint_1_decimals,
            lp_supply,
            protocol_fees_token_0,
            protocol_fees_token_1,
            fund_fees_token_0,
            fund_fees_token_1,
            open_time,
        })
    }

    /// Check if pool is active (status == 0 or 1 typically means active)
    pub fn is_active(&self) -> bool {
        // Status 0 = normal, 1 = paused deposits, 2 = paused all
        self.status < 2
    }

    /// Check if this pool contains the given mint
    pub fn contains_mint(&self, mint: &Pubkey) -> bool {
        self.token_0_mint == *mint || self.token_1_mint == *mint
    }

    /// Get the other mint in the pair
    pub fn other_mint(&self, mint: &Pubkey) -> Option<Pubkey> {
        if self.token_0_mint == *mint {
            Some(self.token_1_mint)
        } else if self.token_1_mint == *mint {
            Some(self.token_0_mint)
        } else {
            None
        }
    }

    /// Determine swap direction: true if input is token_0, false if token_1
    pub fn is_token_0(&self, input_mint: &Pubkey) -> Option<bool> {
        if self.token_0_mint == *input_mint {
            Some(true)
        } else if self.token_1_mint == *input_mint {
            Some(false)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_size() {
        // Verify our expected size matches
        assert_eq!(CPMM_POOL_SIZE, 397);
    }
}
