//! Meteora DLMM swap instruction builder
//!
//! GEYSER-FIRST: Builds swap instructions using only cached data from Geyser.
//! NO RPC CALLS IN HOT PATH!
//!
//! The active_id and pool state come from LivePoolCache (Geyser subscription).
//! Bin array PDAs are derived deterministically - if they don't exist,
//! TX simulation will fail with a clear error.
//!
//! Key accounts:
//! - `bin_array_bitmap_extension`: PDA for pools (always derived, no RPC check)
//!   Seeds: ["bitmap_extension", lb_pair.as_ref()]
//! - `bin_arrays`: PDAs derived from active_id (3 arrays: active-1, active, active+1)
//!   Seeds: ["bin_array", lb_pair.as_ref(), &index.to_le_bytes()]

use anyhow::{ensure, Result};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use std::str::FromStr;
use std::sync::Arc;

use crate::solana::rpc::SolanaRpc;
use tracing::debug;

/// SPL Token program ID
pub const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// Meteora DLMM program ID
pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

/// Instruction discriminator for swap (first 8 bytes of instruction data)
/// Anchor: sha256("global:swap")[0..8]
const SWAP_DISCRIMINATOR: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

/// Swap direction
#[derive(Debug, Clone, Copy)]
pub enum SwapDirection {
    /// Swap X for Y (buy Y with X)
    XtoY,
    /// Swap Y for X (buy X with Y)
    YtoX,
}

/// Builder for Meteora DLMM swap instructions (GEYSER-FIRST: no RPC in hot path)
pub struct MeteoraDlmmSwapBuilder {
    #[allow(dead_code)] // RPC kept for potential future use outside hot path
    rpc: Arc<SolanaRpc>,
}

impl MeteoraDlmmSwapBuilder {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self { rpc }
    }

    /// Derive the bin_array_bitmap_extension PDA for a pool
    /// 
    /// This account is required for pools with extended price range (liquidity across > 512 arrays).
    /// Seeds: ["bitmap_extension", lb_pair.as_ref()]
    pub fn derive_bitmap_extension_pda(lb_pair: &Pubkey) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let (pda, _bump) = Pubkey::find_program_address(
            &[b"bitmap_extension", lb_pair.as_ref()],
            &program_id,
        );
        Ok(pda)
    }

    /// Derive bin array PDAs for a given active_id (GEYSER-FIRST: no RPC!)
    ///
    /// Instead of checking which bin arrays exist via RPC, we simply derive
    /// all potentially needed bin array PDAs and include them in the TX.
    /// The Solana runtime will validate them. If a bin array doesn't exist,
    /// the TX simulation will fail with a clear error - that's fine!
    ///
    /// Why this is correct:
    /// - Geyser provides real-time active_id updates (faster than RPC)
    /// - Bin array PDAs are deterministic (seed = [b"bin_array", lb_pair, index])
    /// - Including a non-existent PDA causes simulation failure (not silent error)
    /// - 3 bin arrays (active-1, active, active+1) cover typical swap ranges
    pub fn derive_bin_arrays_for_active_id(lb_pair: &Pubkey, active_id: i32) -> Result<Vec<Pubkey>> {
        let active_array_index = Self::bin_id_to_bin_array_index(active_id);
        
        // Include active + adjacent bin arrays to handle edge cases
        let indices: Vec<i64> = vec![
            active_array_index - 1,
            active_array_index,
            active_array_index + 1,
        ];
        
        let mut bin_arrays = Vec::with_capacity(3);
        for index in indices {
            let pda = Self::derive_bin_array_pda(lb_pair, index)?;
            bin_arrays.push(pda);
        }
        
        debug!(
            pool = %lb_pair,
            active_id = active_id,
            active_array_index = active_array_index,
            bin_arrays_count = bin_arrays.len(),
            "Meteora: derived bin array PDAs (GEYSER-FIRST, no RPC)"
        );
        
        Ok(bin_arrays)
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
        direction: SwapDirection,
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
        // 4. user_token_in (writable) - depends on swap direction!
        // 5. user_token_out (writable) - depends on swap direction!
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

        // CRITICAL: Accounts 4 and 5 are user_token_in and user_token_out, NOT x/y!
        // The order depends on swap direction:
        // - XtoY (sell Token for SOL): in=token_x, out=token_y
        // - YtoX (buy Token with SOL): in=token_y (wSOL), out=token_x (Token)
        let (user_token_in, user_token_out) = match direction {
            SwapDirection::XtoY => (user_token_x, user_token_y),
            SwapDirection::YtoX => (user_token_y, user_token_x),
        };

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
            AccountMeta::new(*user_token_in, false), // 4: User token IN (writable)
            AccountMeta::new(*user_token_out, false), // 5: User token OUT (writable)
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
    /// GEYSER-FIRST: This method uses the active_id from the LivePoolCache (via Geyser)
    /// and derives bin array PDAs deterministically. NO RPC CALLS IN HOT PATH!
    ///
    /// The active_id from Geyser is MORE CURRENT than RPC (push vs pull).
    /// Bin arrays are derived PDAs - if one doesn't exist, simulation will fail cleanly.
    #[allow(clippy::too_many_arguments)]
    pub fn build_swap_with_bins_sync(
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
        direction: SwapDirection,
        active_id: i32,
        _bin_step: u16,
    ) -> Result<Instruction> {
        ensure!(amount_in > 0, "Amount in must be positive");
        ensure!(min_amount_out > 0, "Min amount out must be positive");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let token_program = Pubkey::from_str(TOKEN_PROGRAM)?;

        // GEYSER-FIRST: Use active_id directly from LivePoolCache (no RPC!)
        // The Geyser subscription provides real-time updates - more current than RPC.
        debug!(
            pool = %lb_pair,
            active_id = active_id,
            "Meteora: using active_id from Geyser cache (GEYSER-FIRST)"
        );

        // bin_array_bitmap_extension is OPTIONAL in Meteora DLMM.
        // Per official Meteora SDK pattern: use program_id when extension doesn't exist.
        // See: https://github.com/MeteoraAg/dlmm-sdk cli/src/instructions/swap_exact_in.rs
        //   bitmap_extension.map(|_| key).or(Some(dlmm::ID))
        //
        // If we pass a derived PDA that doesn't exist on-chain, Anchor will fail with:
        //   "AccountOwnedByWrongProgram" (error 3007) because non-existent PDAs
        //   are owned by System Program, not DLMM Program.
        //
        // SAFE DEFAULT: Use program_id (pools with extended price range are rare for new tokens)
        // TODO: Track bitmap extension existence in LivePoolCache via Geyser for full accuracy
        let bin_array_bitmap_extension = program_id;

        // Derive bin array PDAs (GEYSER-FIRST: no RPC to check existence!)
        // If a bin array doesn't exist, the TX simulation will fail with a clear error.
        let bin_arrays = Self::derive_bin_arrays_for_active_id(lb_pair, active_id)?;

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
        // CRITICAL: Accounts 4 and 5 are user_token_in and user_token_out, NOT x/y!
        // The order depends on swap direction:
        // - XtoY (sell Token for SOL): in=token_x, out=token_y
        // - YtoX (buy Token with SOL): in=token_y (wSOL), out=token_x (Token)
        let (user_token_in, user_token_out) = match direction {
            SwapDirection::XtoY => (user_token_x, user_token_y),
            SwapDirection::YtoX => (user_token_y, user_token_x),
        };
        
        let mut accounts = vec![
            AccountMeta::new(*lb_pair, false),      // 0: LB Pair (writable)
            AccountMeta::new_readonly(bin_array_bitmap_extension, false), // 1: bitmap extension (required if exists!)
            AccountMeta::new(*reserve_x, false),    // 2: Reserve X (writable)
            AccountMeta::new(*reserve_y, false),    // 3: Reserve Y (writable)
            AccountMeta::new(*user_token_in, false), // 4: User token IN (writable) - depends on direction!
            AccountMeta::new(*user_token_out, false), // 5: User token OUT (writable) - depends on direction!
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
        // bin_arrays is now Vec<Pubkey> from fetch_bin_arrays_direct
        for bin_array_pda in bin_arrays {
            accounts.push(AccountMeta::new(bin_array_pda, false));
        }

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
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
    /// Bin array index = floor(bin_id / BIN_ARRAY_SIZE)
    ///
    /// CRITICAL: Must use floor division for negative bin_ids!
    /// - bin_id = -1:  floor(-1/70) = -1  (correct)
    /// - bin_id = -1:  -1 / 70 = 0       (wrong! integer division truncates toward zero)
    pub fn bin_id_to_bin_array_index(bin_id: i32) -> i64 {
        const BIN_ARRAY_SIZE: i32 = 70; // Meteora default
        // Use div_euclid for floor division (handles negative numbers correctly)
        bin_id.div_euclid(BIN_ARRAY_SIZE) as i64
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

        // Negative bins - MUST use floor division!
        // bin_id = -1:  floor(-1/70) = -1
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(-1), -1);
        // bin_id = -70: floor(-70/70) = -1
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(-70), -1);
        // bin_id = -71: floor(-71/70) = -2
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(-71), -2);
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
