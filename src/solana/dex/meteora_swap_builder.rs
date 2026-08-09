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
//! - `bin_array_bitmap_extension`: optional PDA when pool has extended bin-array bitmap
//!   Seeds: ["bitmap", lb_pair.as_ref()] (official Meteora DLMM)
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
use tracing::{debug, info};

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

/// Bin-array coverage for DLMM swap remaining accounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinArrayCoverage {
    /// Active bin array only — for size-constrained atomic bundles (small swaps).
    ActiveOnly,
    /// Active ± 1 — default for general swaps.
    AdjacentThree,
}

/// Extra-data key for [`MeteoraDlmm`] bin-array coverage (cross-dex atomic bundles).
pub const METEORA_BIN_ARRAY_COVERAGE_KEY: &str = "bin_array_coverage";
/// Value: use [`BinArrayCoverage::ActiveOnly`] for minimal TX size.
pub const METEORA_BIN_ARRAY_COVERAGE_ACTIVE_ONLY: &str = "active_only";

/// Extra-data key: pool has on-chain `BinArrayBitmapExtension` (`"true"` / `"false"`).
pub const METEORA_HAS_BITMAP_EXTENSION_KEY: &str = "has_bitmap_extension";

/// Anchor discriminator for `BinArrayBitmapExtension` (Meteora DLMM).
pub const METEORA_BITMAP_EXTENSION_DISCRIMINATOR: [u8; 8] = [80, 111, 124, 113, 55, 237, 18, 5];

/// On-chain account size: 8 + 32 + (12 * 8 * 8) * 2 bitmap planes.
pub const METEORA_BITMAP_EXTENSION_ACCOUNT_SIZE: usize = 1576;

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
    /// Required when the pool has initialized `BinArrayBitmapExtension` on-chain.
    /// Seeds: ["bitmap", lb_pair.as_ref()] (official Meteora DLMM IDL).
    pub fn derive_bitmap_extension_pda(lb_pair: &Pubkey) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let (pda, _bump) =
            Pubkey::find_program_address(&[b"bitmap", lb_pair.as_ref()], &program_id);
        Ok(pda)
    }

    /// Resolve account #1 (`bin_array_bitmap_extension`) for swap instructions.
    ///
    /// - `Some(true)` → derived bitmap-extension PDA (pool has extension on-chain)
    /// - `Some(false)` | `None` → program_id placeholder (safe default per Meteora SDK)
    pub fn resolve_bin_array_bitmap_extension(
        lb_pair: &Pubkey,
        has_bitmap_extension: Option<bool>,
    ) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        if has_bitmap_extension == Some(true) {
            Self::derive_bitmap_extension_pda(lb_pair)
        } else {
            Ok(program_id)
        }
    }

    /// Parse parent LB pair from a Geyser `BinArrayBitmapExtension` account update.
    pub fn parse_bitmap_extension_lb_pair(data: &[u8]) -> Option<Pubkey> {
        if data.len() < 40 {
            return None;
        }
        if data[0..8] != METEORA_BITMAP_EXTENSION_DISCRIMINATOR {
            return None;
        }
        Pubkey::try_from(&data[8..40]).ok()
    }

    /// Heuristic: METEORA DLMM owner + bitmap-extension account layout.
    pub fn geyser_account_data_looks_like_meteora_bitmap_extension(
        owner: &Pubkey,
        data: &[u8],
    ) -> bool {
        let Ok(program_id) = Pubkey::from_str(METEORA_DLMM_PROGRAM) else {
            return false;
        };
        *owner == program_id
            && data.len() >= METEORA_BITMAP_EXTENSION_ACCOUNT_SIZE
            && data[0..8] == METEORA_BITMAP_EXTENSION_DISCRIMINATOR
    }

    /// Patch swap instruction account #1 after build (cross-DEX / cache-aware path).
    pub fn patch_swap_ix_bitmap_extension(
        ix: &mut Instruction,
        lb_pair: &Pubkey,
        has_bitmap_extension: Option<bool>,
    ) -> Result<()> {
        let bitmap = Self::resolve_bin_array_bitmap_extension(lb_pair, has_bitmap_extension)?;
        if ix.accounts.len() > 1 {
            ix.accounts[1] = AccountMeta::new_readonly(bitmap, false);
        }
        Ok(())
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
    pub fn derive_bin_arrays_for_active_id(
        lb_pair: &Pubkey,
        active_id: i32,
    ) -> Result<Vec<Pubkey>> {
        Self::derive_bin_arrays_for_active_id_with_coverage(
            lb_pair,
            active_id,
            BinArrayCoverage::AdjacentThree,
        )
    }

    /// Derive bin-array PDAs with explicit coverage (GEYSER-FIRST: no RPC).
    pub fn derive_bin_arrays_for_active_id_with_coverage(
        lb_pair: &Pubkey,
        active_id: i32,
        coverage: BinArrayCoverage,
    ) -> Result<Vec<Pubkey>> {
        let active_array_index = Self::bin_id_to_bin_array_index(active_id);

        let indices: Vec<i64> = match coverage {
            BinArrayCoverage::ActiveOnly => vec![active_array_index],
            BinArrayCoverage::AdjacentThree => vec![
                active_array_index - 1,
                active_array_index,
                active_array_index + 1,
            ],
        };

        let mut bin_arrays = Vec::with_capacity(indices.len());
        for index in indices {
            let pda = Self::derive_bin_array_pda(lb_pair, index)?;
            bin_arrays.push(pda);
        }

        debug!(
            pool = %lb_pair,
            active_id = active_id,
            active_array_index = active_array_index,
            coverage = ?coverage,
            bin_arrays_count = bin_arrays.len(),
            "Meteora: derived bin array PDAs (GEYSER-FIRST, no RPC)"
        );

        Ok(bin_arrays)
    }

    /// Build a swap instruction for Meteora DLMM
    ///
    /// # Arguments
    /// * `has_bitmap_extension` - Geyser-tracked extension existence (`None` → program_id placeholder)
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
        has_bitmap_extension: Option<bool>,
    ) -> Result<Instruction> {
        ensure!(amount_in > 0, "Amount in must be positive");
        ensure!(min_amount_out > 0, "Min amount out must be positive");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let token_program = Pubkey::from_str(TOKEN_PROGRAM)?;

        let bin_array_bitmap_extension =
            Self::resolve_bin_array_bitmap_extension(lb_pair, has_bitmap_extension)?;

        // Derive bin array PDA for active bin
        let bin_array_index = Self::bin_id_to_bin_array_index(active_id);
        let bin_array_pda = Self::derive_bin_array_pda(lb_pair, bin_array_index)?;

        // Build instruction data
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&SWAP_DISCRIMINATOR);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());

        let (user_token_in, user_token_out) = match direction {
            SwapDirection::XtoY => (user_token_x, user_token_y),
            SwapDirection::YtoX => (user_token_y, user_token_x),
        };

        let (oracle, _) = Pubkey::find_program_address(&[b"oracle", lb_pair.as_ref()], &program_id);
        let (event_authority, _) =
            Pubkey::find_program_address(&[b"__event_authority"], &program_id);

        let accounts = vec![
            AccountMeta::new(*lb_pair, false),
            AccountMeta::new_readonly(bin_array_bitmap_extension, false),
            AccountMeta::new(*reserve_x, false),
            AccountMeta::new(*reserve_y, false),
            AccountMeta::new(*user_token_in, false),
            AccountMeta::new(*user_token_out, false),
            AccountMeta::new_readonly(*token_x_mint, false),
            AccountMeta::new_readonly(*token_y_mint, false),
            AccountMeta::new(oracle, false),
            AccountMeta::new_readonly(program_id, false),
            AccountMeta::new_readonly(*user, true),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(event_authority, false),
            AccountMeta::new_readonly(program_id, false),
            AccountMeta::new(bin_array_pda, false),
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
    ///
    /// CRITICAL: token_x_program and token_y_program must match the actual token programs
    /// of the mints (SPL Token or Token-2022). Mismatch causes InvalidAccountData errors.
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
        token_x_program: &Pubkey,
        token_y_program: &Pubkey,
        user: &Pubkey,
        amount_in: u64,
        min_amount_out: u64,
        direction: SwapDirection,
        active_id: i32,
        _bin_step: u16,
    ) -> Result<Instruction> {
        self.build_swap_with_bins_sync_coverage(
            lb_pair,
            reserve_x,
            reserve_y,
            user_token_x,
            user_token_y,
            token_x_mint,
            token_y_mint,
            token_x_program,
            token_y_program,
            user,
            amount_in,
            min_amount_out,
            direction,
            active_id,
            _bin_step,
            BinArrayCoverage::AdjacentThree,
            None,
        )
    }

    /// Same as [`build_swap_with_bins_sync`] but with explicit bin-array coverage.
    #[allow(clippy::too_many_arguments)]
    pub fn build_swap_with_bins_sync_coverage(
        &self,
        lb_pair: &Pubkey,
        reserve_x: &Pubkey,
        reserve_y: &Pubkey,
        user_token_x: &Pubkey,
        user_token_y: &Pubkey,
        token_x_mint: &Pubkey,
        token_y_mint: &Pubkey,
        token_x_program: &Pubkey,
        token_y_program: &Pubkey,
        user: &Pubkey,
        amount_in: u64,
        min_amount_out: u64,
        direction: SwapDirection,
        active_id: i32,
        _bin_step: u16,
        bin_array_coverage: BinArrayCoverage,
        has_bitmap_extension: Option<bool>,
    ) -> Result<Instruction> {
        ensure!(amount_in > 0, "Amount in must be positive");
        ensure!(min_amount_out > 0, "Min amount out must be positive");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;

        // Log token programs being used (important for Token-2022 debugging)
        let token_2022_id = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;
        let x_is_2022 = token_x_program == &token_2022_id;
        let y_is_2022 = token_y_program == &token_2022_id;
        if x_is_2022 || y_is_2022 {
            info!(
                pool = %lb_pair,
                token_x_program = %token_x_program,
                token_y_program = %token_y_program,
                x_is_token_2022 = x_is_2022,
                y_is_token_2022 = y_is_2022,
                "Meteora Swap Builder: using Token-2022 programs"
            );
        }

        let active_array_index = Self::bin_id_to_bin_array_index(active_id);
        info!(
            pool = %lb_pair,
            active_id = active_id,
            active_array_index = active_array_index,
            has_bitmap_extension = ?has_bitmap_extension,
            "Meteora: building swap with active_id from Geyser cache (GEYSER-FIRST)"
        );

        let bin_array_bitmap_extension =
            Self::resolve_bin_array_bitmap_extension(lb_pair, has_bitmap_extension)?;

        // Derive bin array PDAs (GEYSER-FIRST: no RPC to check existence!)
        // If a bin array doesn't exist, the TX simulation will fail with a clear error.
        let bin_arrays = Self::derive_bin_arrays_for_active_id_with_coverage(
            lb_pair,
            active_id,
            bin_array_coverage,
        )?;

        // Log the derived bin arrays for debugging
        info!(
            pool = %lb_pair,
            bin_array_count = bin_arrays.len(),
            bin_array_0 = %bin_arrays.first().map(|p| p.to_string()).unwrap_or_default(),
            bin_array_1 = %bin_arrays.get(1).map(|p| p.to_string()).unwrap_or_default(),
            bin_array_2 = %bin_arrays.get(2).map(|p| p.to_string()).unwrap_or_default(),
            "Meteora: derived bin array PDAs (may not exist on-chain!)"
        );

        // Derive oracle PDA
        let (oracle, _) = Pubkey::find_program_address(&[b"oracle", lb_pair.as_ref()], &program_id);

        // Derive event_authority PDA
        let (event_authority, _) =
            Pubkey::find_program_address(&[b"__event_authority"], &program_id);

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
            AccountMeta::new(*lb_pair, false), // 0: LB Pair (writable)
            AccountMeta::new_readonly(bin_array_bitmap_extension, false), // 1: bitmap extension (required if exists!)
            AccountMeta::new(*reserve_x, false),                          // 2: Reserve X (writable)
            AccountMeta::new(*reserve_y, false),                          // 3: Reserve Y (writable)
            AccountMeta::new(*user_token_in, false), // 4: User token IN (writable) - depends on direction!
            AccountMeta::new(*user_token_out, false), // 5: User token OUT (writable) - depends on direction!
            AccountMeta::new_readonly(*token_x_mint, false), // 6: Token X mint
            AccountMeta::new_readonly(*token_y_mint, false), // 7: Token Y mint
            AccountMeta::new(oracle, false),          // 8: Oracle PDA (WRITABLE!)
            AccountMeta::new_readonly(program_id, false), // 9: host_fee_in (optional)
            AccountMeta::new_readonly(*user, true),   // 10: User (signer)
            AccountMeta::new_readonly(*token_x_program, false), // 11: token_x_program (SPL Token OR Token-2022)
            AccountMeta::new_readonly(*token_y_program, false), // 12: token_y_program (SPL Token OR Token-2022)
            AccountMeta::new_readonly(event_authority, false),  // 13: Event authority
            AccountMeta::new_readonly(program_id, false),       // 14: Program ID
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

    #[test]
    fn active_only_bin_coverage_has_fewer_accounts_than_adjacent_three() {
        let lb_pair = Pubkey::new_unique();
        let active_only = MeteoraDlmmSwapBuilder::derive_bin_arrays_for_active_id_with_coverage(
            &lb_pair,
            100,
            BinArrayCoverage::ActiveOnly,
        )
        .expect("active only");
        let adjacent = MeteoraDlmmSwapBuilder::derive_bin_arrays_for_active_id_with_coverage(
            &lb_pair,
            100,
            BinArrayCoverage::AdjacentThree,
        )
        .expect("adjacent three");
        assert_eq!(active_only.len(), 1);
        assert_eq!(adjacent.len(), 3);
    }

    #[test]
    fn bitmap_extension_pda_uses_official_bitmap_seed() {
        let lb_pair = Pubkey::from_str("2TD1fMPg2w7Hjt8bASSdxi92YFNQFgvdznqVApe3NGpn").unwrap();
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM).unwrap();
        let (official, _) =
            Pubkey::find_program_address(&[b"bitmap", lb_pair.as_ref()], &program_id);
        let derived = MeteoraDlmmSwapBuilder::derive_bitmap_extension_pda(&lb_pair).unwrap();
        assert_eq!(derived, official);
        let (legacy_wrong, _) =
            Pubkey::find_program_address(&[b"bitmap_extension", lb_pair.as_ref()], &program_id);
        assert_ne!(derived, legacy_wrong);
    }

    #[test]
    fn resolve_bitmap_extension_placeholder_vs_pda() {
        let lb_pair = Pubkey::new_unique();
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM).unwrap();
        let placeholder =
            MeteoraDlmmSwapBuilder::resolve_bin_array_bitmap_extension(&lb_pair, None).unwrap();
        assert_eq!(placeholder, program_id);
        let false_placeholder =
            MeteoraDlmmSwapBuilder::resolve_bin_array_bitmap_extension(&lb_pair, Some(false))
                .unwrap();
        assert_eq!(false_placeholder, program_id);
        let pda = MeteoraDlmmSwapBuilder::resolve_bin_array_bitmap_extension(&lb_pair, Some(true))
            .unwrap();
        assert_ne!(pda, program_id);
        assert_eq!(
            pda,
            MeteoraDlmmSwapBuilder::derive_bitmap_extension_pda(&lb_pair).unwrap()
        );
    }

    #[test]
    fn patch_swap_ix_bitmap_extension_updates_account_one() {
        let lb_pair = Pubkey::new_unique();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let builder = MeteoraDlmmSwapBuilder::new(rpc);
        let user = Pubkey::new_unique();
        let mut ix = builder
            .build_swap_with_bins_sync_coverage(
                &lb_pair,
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::from_str(TOKEN_PROGRAM).unwrap(),
                &Pubkey::from_str(TOKEN_PROGRAM).unwrap(),
                &user,
                1,
                1,
                SwapDirection::YtoX,
                100,
                10,
                BinArrayCoverage::AdjacentThree,
                None,
            )
            .expect("build swap");
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM).unwrap();
        assert_eq!(ix.accounts[1].pubkey, program_id);
        MeteoraDlmmSwapBuilder::patch_swap_ix_bitmap_extension(&mut ix, &lb_pair, Some(true))
            .expect("patch");
        assert_eq!(
            ix.accounts[1].pubkey,
            MeteoraDlmmSwapBuilder::derive_bitmap_extension_pda(&lb_pair).unwrap()
        );
    }
}
