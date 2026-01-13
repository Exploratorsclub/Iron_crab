//! Meteora DLMM swap instruction builder
//!
//! Builds swap instructions for Meteora's Dynamic Liquidity Market Maker.
//! Handles bin array discovery and proper account ordering.
//!
//! Key accounts:
//! - `bin_array_bitmap_extension`: Optional PDA for pools with extended price range
//!   Seeds: ["bitmap_extension", lb_pair.as_ref()]
//!   Required when pool liquidity spans > 512 bin arrays (rare for most pools)

use anyhow::{anyhow, ensure, Result};
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
use tracing::{debug, info, warn};

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

/// Builder for Meteora DLMM swap instructions
pub struct MeteoraDlmmSwapBuilder {
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

    /// Check if bitmap extension account exists on-chain
    async fn check_bitmap_extension_exists(&self, lb_pair: &Pubkey) -> bool {
        let pda = match Self::derive_bitmap_extension_pda(lb_pair) {
            Ok(p) => p,
            Err(_) => return false,
        };
        
        // Try to fetch the account - if it exists, we need to include it
        match self.rpc.get_account_retry(&pda).await {
            Ok(account) => {
                // Account exists and has data
                !account.data.is_empty()
            }
            Err(_) => false,
        }
    }

    /// Fetch the CURRENT active_id from the pool account on-chain.
    ///
    /// This is critical because the Intent's active_id may be stale -
    /// prices can move between Intent creation and execution.
    /// Using the wrong active_id leads to fetching wrong bin_arrays → Error 3005.
    ///
    /// If we can't fetch the pool, we fall back to the Intent's active_id.
    async fn fetch_current_active_id(&self, lb_pair: &Pubkey, fallback_active_id: i32) -> Result<i32> {
        // LB Pair account layout: active_id is at offset 0x30 (48 decimal), 4 bytes i32 LE
        const ACTIVE_ID_OFFSET: usize = 0x30;
        const MIN_ACCOUNT_SIZE: usize = ACTIVE_ID_OFFSET + 4;

        match self.rpc.get_account_retry(lb_pair).await {
            Ok(account) => {
                if account.data.len() >= MIN_ACCOUNT_SIZE {
                    let active_id_bytes: [u8; 4] = account.data[ACTIVE_ID_OFFSET..ACTIVE_ID_OFFSET + 4]
                        .try_into()
                        .map_err(|_| anyhow!("Failed to read active_id bytes from pool"))?;
                    let current_active_id = i32::from_le_bytes(active_id_bytes);
                    
                    if current_active_id != fallback_active_id {
                        info!(
                            pool = %lb_pair,
                            intent_active_id = fallback_active_id,
                            current_active_id = current_active_id,
                            delta = current_active_id - fallback_active_id,
                            "Meteora: active_id changed since Intent creation - using current on-chain value"
                        );
                    }
                    
                    Ok(current_active_id)
                } else {
                    warn!(
                        pool = %lb_pair,
                        account_size = account.data.len(),
                        required_size = MIN_ACCOUNT_SIZE,
                        "Meteora: pool account too small to read active_id, using Intent fallback"
                    );
                    Ok(fallback_active_id)
                }
            }
            Err(e) => {
                warn!(
                    pool = %lb_pair,
                    error = %e,
                    "Meteora: failed to fetch pool for current active_id, using Intent fallback"
                );
                Ok(fallback_active_id)
            }
        }
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
        active_id_from_intent: i32,
        _bin_step: u16,
    ) -> Result<Instruction> {
        ensure!(amount_in > 0, "Amount in must be positive");
        ensure!(min_amount_out > 0, "Min amount out must be positive");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let token_program = Pubkey::from_str(TOKEN_PROGRAM)?;

        // CRITICAL: Fetch CURRENT active_id from pool on-chain!
        // The Intent's active_id may be stale (price moved since Intent creation).
        // If we use stale active_id, we fetch wrong bin_arrays → Error 3005.
        let active_id = self.fetch_current_active_id(lb_pair, active_id_from_intent).await?;

        // Check if bitmap extension account exists for this pool
        // The bitmap extension is required for pools with extended price range (> 512 arrays)
        // If it exists on-chain, we MUST include it; otherwise use program_id as placeholder
        let bitmap_extension_pda = Self::derive_bitmap_extension_pda(lb_pair)?;
        let bitmap_extension_exists = self.check_bitmap_extension_exists(lb_pair).await;
        let bin_array_bitmap_extension = if bitmap_extension_exists {
            debug!(
                pool = %lb_pair,
                bitmap_extension = %bitmap_extension_pda,
                "Meteora pool has bitmap extension account - including in TX"
            );
            bitmap_extension_pda
        } else {
            debug!(
                pool = %lb_pair,
                "Meteora pool has no bitmap extension - using program_id placeholder"
            );
            program_id
        };

        // Fetch bin arrays for this pool (direct PDA derivation)
        let bin_arrays = self.fetch_bin_arrays_direct(lb_pair, active_id).await?;
        
        // Meteora DLMM requires at least 1 bin array to be present
        if bin_arrays.is_empty() {
            return Err(anyhow!(
                "No bin arrays found for pool {} (active_id={}). \
                 Bin array PDAs might not exist on-chain yet.",
                lb_pair, active_id
            ));
        }

        debug!(
            pool = %lb_pair,
            active_id = active_id,
            bin_arrays_count = bin_arrays.len(),
            "Meteora: fetched bin arrays for swap"
        );

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
            AccountMeta::new_readonly(bin_array_bitmap_extension, false), // 1: bitmap extension (required if exists!)
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

    /// Fetch bin array accounts for a pool (DEPRECATED - use fetch_bin_arrays_direct)
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

    /// Fetch bin arrays by direct PDA derivation (more reliable than getProgramAccounts)
    ///
    /// This method derives bin array PDAs directly from the active_id and fetches them
    /// via individual getAccount calls. This is more reliable than getProgramAccounts
    /// which can be rate-limited or return incomplete results.
    ///
    /// We fetch bin arrays for indices [active_index - 1, active_index, active_index + 1]
    /// to cover typical swap ranges. All three are needed because:
    /// 1. active_id might be at the edge of a bin array
    /// 2. Swaps can cross multiple bin arrays depending on liquidity distribution
    /// 3. Meteora DLMM requires all touched bin arrays to be present
    pub async fn fetch_bin_arrays_direct(
        &self,
        lb_pair: &Pubkey,
        active_id: i32,
    ) -> Result<Vec<Pubkey>> {
        let active_array_index = Self::bin_id_to_bin_array_index(active_id);

        // Fetch active + adjacent bin arrays to handle swaps that cross array boundaries.
        // This is critical for Meteora - if the swap touches a bin array not included,
        // we get Error 3005 (AccountNotEnoughKeys).
        let indices_to_check: Vec<i64> = vec![
            active_array_index - 1,
            active_array_index,
            active_array_index + 1,
        ];

        let mut found_arrays: Vec<Pubkey> = Vec::new();

        for index in indices_to_check {
            let pda = Self::derive_bin_array_pda(lb_pair, index)?;

            match self.rpc.get_account_retry(&pda).await {
                Ok(account) => {
                    // Bin array accounts are typically ~10KB+
                    if account.data.len() >= 100 {
                        debug!(
                            pool = %lb_pair,
                            bin_array_index = index,
                            pda = %pda,
                            "Found bin array on-chain"
                        );
                        found_arrays.push(pda);
                    }
                }
                Err(_) => {
                    // Bin array doesn't exist - this is normal for arrays outside liquidity range
                    debug!(
                        pool = %lb_pair,
                        bin_array_index = index,
                        "Bin array not found on-chain (expected if outside liquidity range)"
                    );
                }
            }
        }

        if found_arrays.is_empty() {
            warn!(
                pool = %lb_pair,
                active_id = active_id,
                active_array_index = active_array_index,
                "No bin arrays found - pool might have no liquidity at current price"
            );
        } else {
            info!(
                pool = %lb_pair,
                active_id = active_id,
                active_array_index = active_array_index,
                found_count = found_arrays.len(),
                bin_arrays = ?found_arrays,
                "Meteora: fetched bin arrays for swap"
            );
        }

        Ok(found_arrays)
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
