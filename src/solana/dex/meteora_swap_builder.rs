//! Meteora DLMM swap instruction builder
//!
//! Builds swap instructions for Meteora's Dynamic Liquidity Market Maker.
//! Handles bin array discovery and proper account ordering.

use anyhow::{ensure, Result};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use std::str::FromStr;
use std::sync::Arc;

use super::meteora_bin_array_layout::BinArray;
use crate::solana::rpc::SolanaRpc;
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};

/// SPL Token program ID
pub const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// Meteora DLMM program ID
pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

/// Instruction discriminator for swap (first 8 bytes of instruction data)
/// This is a placeholder - needs to be reverse-engineered from mainnet transactions
const SWAP_DISCRIMINATOR: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

/// Swap direction
#[derive(Debug, Clone, Copy)]
pub enum SwapDirection {
    /// Swap X for Y (buy Y with X)
    XtoY,
    /// Swap Y for X (buy X with Y)
    YtoX,
}

/// Builder for Meteora DLMM swap instructions
pub struct MeteoraDlmmSwapBuilder {
    rpc: Arc<SolanaRpc>,
}

impl MeteoraDlmmSwapBuilder {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self { rpc }
    }

    /// Build a swap instruction for Meteora DLMM
    ///
    /// # Arguments
    /// * `lb_pair` - LB Pair account (pool)
    /// * `reserve_x` - Token X vault
    /// * `reserve_y` - Token Y vault
    /// * `user_token_x` - User's token X account (ATA)
    /// * `user_token_y` - User's token Y account (ATA)
    /// * `user` - User's wallet pubkey
    /// * `amount_in` - Input amount (raw units)
    /// * `min_amount_out` - Minimum output amount (slippage protection)
    /// * `_direction` - Swap direction (X→Y or Y→X)
    pub fn build_swap(
        &self,
        lb_pair: &Pubkey,
        reserve_x: &Pubkey,
        reserve_y: &Pubkey,
        user_token_x: &Pubkey,
        user_token_y: &Pubkey,
        user: &Pubkey,
        amount_in: u64,
        min_amount_out: u64,
        _direction: SwapDirection,
    ) -> Result<Instruction> {
        ensure!(amount_in > 0, "Amount in must be positive");
        ensure!(min_amount_out > 0, "Min amount out must be positive");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let token_program = Pubkey::from_str(TOKEN_PROGRAM)?;

        // Derive bin_array_bitmap_extension PDA
        // Seeds: ["lb_pair", lb_pair.as_ref()]
        let (bin_array_bitmap_extension, _bump) = Pubkey::find_program_address(
            &[b"bitmap", lb_pair.as_ref()],
            &program_id,
        );

        // Build instruction data
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&SWAP_DISCRIMINATOR);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());

        // Account ordering for Meteora DLMM swap:
        // Per Meteora IDL - must include bin_array_bitmap_extension
        let accounts = vec![
            AccountMeta::new(*lb_pair, false),      // LB Pair (writable)
            AccountMeta::new(bin_array_bitmap_extension, false), // Bitmap extension (writable)
            AccountMeta::new(*reserve_x, false),    // Reserve X (writable)
            AccountMeta::new(*reserve_y, false),    // Reserve Y (writable)
            AccountMeta::new(*user_token_x, false), // User token X (writable)
            AccountMeta::new(*user_token_y, false), // User token Y (writable)
            AccountMeta::new_readonly(*user, true), // User (signer)
            AccountMeta::new_readonly(token_program, false), // Token program
            // Note: Bin array accounts should be added via build_swap_with_bins for production
        ];

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build a swap instruction with bin array accounts
    ///
    /// This is the full version that fetches and includes bin array accounts.
    /// Required for production swaps.
    pub async fn build_swap_with_bins(
        &self,
        lb_pair: &Pubkey,
        reserve_x: &Pubkey,
        reserve_y: &Pubkey,
        user_token_x: &Pubkey,
        user_token_y: &Pubkey,
        user: &Pubkey,
        amount_in: u64,
        min_amount_out: u64,
        direction: SwapDirection,
        active_id: i32,
        bin_step: u16,
    ) -> Result<Instruction> {
        ensure!(amount_in > 0, "Amount in must be positive");
        ensure!(min_amount_out > 0, "Min amount out must be positive");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let token_program = Pubkey::from_str(TOKEN_PROGRAM)?;

        // Fetch bin arrays for this pool
        let bin_arrays = self.fetch_bin_arrays(lb_pair, active_id, bin_step).await?;

        // Build instruction data
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&SWAP_DISCRIMINATOR);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());

        // Build account list
        let mut accounts = vec![
            AccountMeta::new(*lb_pair, false),
            AccountMeta::new(*reserve_x, false),
            AccountMeta::new(*reserve_y, false),
            AccountMeta::new(*user_token_x, false),
            AccountMeta::new(*user_token_y, false),
            AccountMeta::new_readonly(*user, true),
            AccountMeta::new_readonly(token_program, false),
        ];

        // Add bin array accounts (writable, as swap will update bin liquidity)
        for bin_array in bin_arrays {
            let bin_array_pda = Self::derive_bin_array_pda(lb_pair, bin_array.index)?;
            accounts.push(AccountMeta::new(bin_array_pda, false));
        }

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Fetch bin array accounts for a pool
    ///
    /// Meteora DLMM stores bins in "Bin Array" accounts. Each bin array contains
    /// a range of bins (typically 70 bins per array). We need to find which
    /// bin arrays contain the active bins for our swap.
    ///
    /// # Arguments
    /// * `lb_pair` - The LB Pair (pool) pubkey
    /// * `active_id` - Current active bin ID
    /// * `bin_step` - Bin step for price calculation
    ///
    /// # Returns
    /// Vector of BinArray structs containing bin data
    pub async fn fetch_bin_arrays(
        &self,
        lb_pair: &Pubkey,
        active_id: i32,
        bin_step: u16,
    ) -> Result<Vec<BinArray>> {
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;

        // Calculate which bin arrays we need based on active_id
        // We fetch ±3 arrays around the active bin to handle larger swaps
        let active_array_index = Self::bin_id_to_bin_array_index(active_id);
        let array_indices: Vec<i64> = (active_array_index - 3..=active_array_index + 3).collect();

        let mut bin_arrays = Vec::new();

        // Method 1: Direct PDA derivation (faster, but needs all possible indices)
        // We try to fetch known PDAs directly
        for index in &array_indices {
            let pda = Self::derive_bin_array_pda(lb_pair, *index)?;

            if let Ok(account) = self.rpc.get_account_retry(&pda).await {
                if account.data.len() >= 56 {
                    if let Ok(bin_array) = BinArray::parse(&account.data, bin_step) {
                        bin_arrays.push(bin_array);
                    }
                }
            }
        }

        // Method 2: getProgramAccounts with memcmp filter (slower, but comprehensive)
        // Only use if direct fetches didn't return enough data
        if bin_arrays.len() < 2 {
            // Filter for bin arrays belonging to this lb_pair
            // lb_pair is at offset 24 in bin array account
            let memcmp = Memcmp::new(
                24, // offset of lb_pair in BinArray
                MemcmpEncodedBytes::Base58(lb_pair.to_string()),
            );

            let config = RpcProgramAccountsConfig {
                filters: Some(vec![RpcFilterType::Memcmp(memcmp)]),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    data_slice: None,
                    commitment: None,
                    min_context_slot: None,
                },
                with_context: None,
                sort_results: None,
            };

            if let Ok(accounts) = self
                .rpc
                .get_program_accounts_with_config_retry(&program_id, config)
                .await
            {
                for (_pubkey, account) in accounts {
                    if let Ok(bin_array) = BinArray::parse(&account.data, bin_step) {
                        // Only include arrays near the active bin
                        if (bin_array.index - active_array_index).abs() <= 3 {
                            bin_arrays.push(bin_array);
                        }
                    }
                }
            }
        }

        Ok(bin_arrays)
    }

    /// Derive bin array PDA for a given bin array index
    ///
    /// Seed format: [b"bin_array", lb_pair.as_ref(), &bin_array_index.to_le_bytes()]
    pub fn derive_bin_array_pda(lb_pair: &Pubkey, bin_array_index: i64) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;

        let (pda, _bump) = Pubkey::find_program_address(
            &[
                b"bin_array",
                lb_pair.as_ref(),
                &bin_array_index.to_le_bytes(),
            ],
            &program_id,
        );

        Ok(pda)
    }

    /// Calculate which bin array index a bin ID belongs to
    ///
    /// Meteora DLMM uses bin arrays of fixed size (typically 70 bins per array).
    /// Bin array index = bin_id / BIN_ARRAY_SIZE
    pub fn bin_id_to_bin_array_index(bin_id: i32) -> i64 {
        const BIN_ARRAY_SIZE: i32 = 70; // Meteora default
        (bin_id / BIN_ARRAY_SIZE) as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bin_array_index_calculation() {
        // Bin 0 -> array 0
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(0), 0);

        // Bin 69 -> array 0 (last bin in first array)
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(69), 0);

        // Bin 70 -> array 1 (first bin in second array)
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(70), 1);

        // Negative bins
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(-1), 0);
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(-70), -1);
    }

    #[test]
    fn test_derive_bin_array_pda() {
        let lb_pair = Pubkey::new_unique();

        // Should derive deterministically
        let pda1 = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&lb_pair, 0).unwrap();
        let pda2 = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&lb_pair, 0).unwrap();
        assert_eq!(pda1, pda2);

        // Different indices should give different PDAs
        let pda3 = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&lb_pair, 1).unwrap();
        assert_ne!(pda1, pda3);
    }
}
