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
    /// * `token_x_mint` - Token X mint address
    /// * `token_y_mint` - Token Y mint address
    /// * `user` - User's wallet pubkey
    /// * `amount_in` - Input amount (raw units)
    /// * `min_amount_out` - Minimum output amount (slippage protection)
    /// * `_direction` - Swap direction (X→Y or Y→X)
    /// * `active_id` - Current active bin ID for bin array derivation
    #[allow(clippy::too_many_arguments)]
    pub fn build_swap(
        &self,
        lb_pair: &Pubkey,
        reserve_x: &Pubkey,
        reserve_y: &Pubkey,
        user_token_x: &Pubkey,
        user_token_y: &Pubkey,
        token_x_mint: &Pubkey,
        token_y_mint: &Pubkey,
        user: &Pubkey,
        amount_in: u64,
        min_amount_out: u64,
        _direction: SwapDirection,
        active_id: i32,
    ) -> Result<Instruction> {
        ensure!(amount_in > 0, "Amount in must be positive");
        ensure!(min_amount_out > 0, "Min amount out must be positive");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let token_program = Pubkey::from_str(TOKEN_PROGRAM)?;

        // bin_array_bitmap_extension is OPTIONAL. Not all pools have it.
        // If it doesn't exist on-chain, we use the program ID as placeholder.
        // For now, we always use program_id as placeholder since we don't query existence.
        let bin_array_bitmap_extension = program_id;

        // Derive bin array PDA for active bin
        // Bin arrays contain 70 bins each, index = floor(active_id / 70)
        let bin_array_index = Self::bin_id_to_bin_array_index(active_id);
        let bin_array_pda = Self::derive_bin_array_pda(lb_pair, bin_array_index)?;

        // Build instruction data
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&SWAP_DISCRIMINATOR);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());

        // Account ordering for Meteora DLMM swap (from official IDL dlmm.json):
        // 0. lb_pair (writable)
        // 1. bin_array_bitmap_extension (optional, use program_id if not needed)
        // 2. reserve_x (writable)
        // 3. reserve_y (writable)
        // 4. user_token_in (writable)
        // 5. user_token_out (writable)
        // 6. token_x_mint
        // 7. token_y_mint
        // 8. oracle (writable, derived PDA)
        // 9. host_fee_in (optional, writable, use program_id if not needed)
        // 10. user (signer)
        // 11. token_x_program (SPL Token or Token-2022)
        // 12. token_y_program (SPL Token or Token-2022)
        // 13. event_authority (derived PDA)
        // 14. program
        // 15+ bin_arrays (writable, remaining accounts)

        // Derive oracle PDA: seeds = ["oracle", lb_pair]
        let (oracle, _) = Pubkey::find_program_address(
            &[b"oracle", lb_pair.as_ref()],
            &program_id,
        );

        // Derive event_authority PDA: seeds = ["__event_authority"]
        let (event_authority, _) = Pubkey::find_program_address(
            &[b"__event_authority"],
            &program_id,
        );

        let accounts = vec![
            AccountMeta::new(*lb_pair, false),      // 0: LB Pair (writable)
            AccountMeta::new_readonly(bin_array_bitmap_extension, false), // 1: bitmap extension (optional)
            AccountMeta::new(*reserve_x, false),    // 2: Reserve X (writable)
            AccountMeta::new(*reserve_y, false),    // 3: Reserve Y (writable)
            AccountMeta::new(*user_token_x, false), // 4: User token X (writable)
            AccountMeta::new(*user_token_y, false), // 5: User token Y (writable)
            AccountMeta::new_readonly(*token_x_mint, false), // 6: Token X mint
            AccountMeta::new_readonly(*token_y_mint, false), // 7: Token Y mint
            AccountMeta::new(oracle, false),        // 8: Oracle PDA (WRITABLE!)
            AccountMeta::new_readonly(program_id, false), // 9: host_fee_in (optional, use program_id)
            AccountMeta::new_readonly(*user, true), // 10: User (signer)
            AccountMeta::new_readonly(token_program, false), // 11: token_x_program
            AccountMeta::new_readonly(token_program, false), // 12: token_y_program
            AccountMeta::new_readonly(event_authority, false), // 13: Event authority
            AccountMeta::new_readonly(program_id, false), // 14: Program ID
            AccountMeta::new(bin_array_pda, false), // 15: Bin array (writable)
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
    #[allow(clippy::too_many_arguments)]
    pub async fn build_swap_with_bins(
        &self,
        lb_pair: &Pubkey,
        reserve_x: &Pubkey,
        reserve_y: &Pubkey,
        user_token_x: &Pubkey,
        user_token_y: &Pubkey,
        token_x_mint: &Pubkey,
        token_y_mint: &Pubkey,
        user: &Pubkey,
        amount_in: u64,
        min_amount_out: u64,
        _direction: SwapDirection,
        active_id: i32,
        bin_step: u16,
    ) -> Result<Instruction> {
        ensure!(amount_in > 0, "Amount in must be positive");
        ensure!(min_amount_out > 0, "Min amount out must be positive");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let token_program = Pubkey::from_str(TOKEN_PROGRAM)?;

        // bin_array_bitmap_extension is OPTIONAL. Not all pools have it.
        let bin_array_bitmap_extension = program_id;

        // Fetch bin arrays for this pool
        let bin_arrays = self.fetch_bin_arrays(lb_pair, active_id, bin_step).await?;

        // Derive oracle PDA
        let (oracle, _) = Pubkey::find_program_address(
            &[b"oracle", lb_pair.as_ref()],
            &program_id,
        );

        // Derive event_authority PDA
        let (event_authority, _) = Pubkey::find_program_address(
            &[b"__event_authority"],
            &program_id,
        );

        // Build instruction data
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&SWAP_DISCRIMINATOR);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());

        // Build account list (matching official IDL order)
        let mut accounts = vec![
            AccountMeta::new(*lb_pair, false),      // 0: LB Pair (writable)
            AccountMeta::new_readonly(bin_array_bitmap_extension, false), // 1: bitmap extension (optional)
            AccountMeta::new(*reserve_x, false),    // 2: Reserve X (writable)
            AccountMeta::new(*reserve_y, false),    // 3: Reserve Y (writable)
            AccountMeta::new(*user_token_x, false), // 4: User token X (writable)
            AccountMeta::new(*user_token_y, false), // 5: User token Y (writable)
            AccountMeta::new_readonly(*token_x_mint, false), // 6: Token X mint
            AccountMeta::new_readonly(*token_y_mint, false), // 7: Token Y mint
            AccountMeta::new(oracle, false),        // 8: Oracle PDA (WRITABLE!)
            AccountMeta::new_readonly(program_id, false), // 9: host_fee_in (optional)
            AccountMeta::new_readonly(*user, true), // 10: User (signer)
            AccountMeta::new_readonly(token_program, false), // 11: token_x_program
            AccountMeta::new_readonly(token_program, false), // 12: token_y_program
            AccountMeta::new_readonly(event_authority, false), // 13: Event authority
            AccountMeta::new_readonly(program_id, false), // 14: Program ID
        ];

        // Add bin array accounts as remaining accounts (writable)
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
