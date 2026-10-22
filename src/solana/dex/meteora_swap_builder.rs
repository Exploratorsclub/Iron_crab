//! Meteora DLMM swap instruction builder
//!
//! GEYSER-FIRST: Builds swap instructions using only cached data from Geyser.
//! NO RPC CALLS IN HOT PATH!

use anyhow::{anyhow, ensure, Result};
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

/// Token-2022 program ID
pub const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// Wrapped SOL mint
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Memo program (swap2)
/// SPL Memo program v2 (Meteora `swap2` remaining account).
pub const MEMO_PROGRAM: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

/// Meteora DLMM program ID
pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

/// Instruction discriminator for legacy swap
const SWAP_DISCRIMINATOR: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

/// Instruction discriminator for swap2 (Token-2022 / memo)
const SWAP2_DISCRIMINATOR: [u8; 8] = [0x41, 0x4b, 0x3f, 0x4c, 0xeb, 0x5b, 0x5b, 0x88];

/// LB pair internal bitmap offset and size
pub const LB_PAIR_BIN_ARRAY_BITMAP_OFFSET: usize = 584;
pub const LB_PAIR_BIN_ARRAY_BITMAP_WORDS: usize = 16;

/// Internal bin-array index range covered by the LB pair bitmap
pub const INTERNAL_BITMAP_INDEX_MIN: i64 = -512;
pub const INTERNAL_BITMAP_INDEX_MAX: i64 = 511;

/// Extension account payload (bytes 40..1576)
pub const METEORA_BITMAP_EXTENSION_PAYLOAD_LEN: usize = 1536;

/// Swap direction
#[derive(Debug, Clone, Copy)]
pub enum SwapDirection {
    /// Swap X for Y (buy Y with X) — search toward smaller array indices
    XtoY,
    /// Swap Y for X (buy X with Y) — search toward larger array indices
    YtoX,
}

/// Bin-array coverage for DLMM swap remaining accounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinArrayCoverage {
    ActiveOnly,
    AdjacentThree,
}

pub const METEORA_BIN_ARRAY_COVERAGE_KEY: &str = "bin_array_coverage";
pub const METEORA_BIN_ARRAY_COVERAGE_ACTIVE_ONLY: &str = "active_only";

/// Extra-data key: pool has on-chain `BinArrayBitmapExtension` (metadata only; not used for account #1).
pub const METEORA_HAS_BITMAP_EXTENSION_KEY: &str = "has_bitmap_extension";

pub const METEORA_BITMAP_EXTENSION_DISCRIMINATOR: [u8; 8] = [80, 111, 124, 113, 55, 237, 18, 5];
pub const METEORA_BITMAP_EXTENSION_ACCOUNT_SIZE: usize = 1576;

/// Builder for Meteora DLMM swap instructions (GEYSER-FIRST: no RPC in hot path)
pub struct MeteoraDlmmSwapBuilder {
    #[allow(dead_code)]
    rpc: Arc<SolanaRpc>,
}

impl MeteoraDlmmSwapBuilder {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self { rpc }
    }

    pub fn derive_bitmap_extension_pda(lb_pair: &Pubkey) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let (pda, _bump) =
            Pubkey::find_program_address(&[b"bitmap", lb_pair.as_ref()], &program_id);
        Ok(pda)
    }

    /// Parse internal `[u64; 16]` bitmap from LB pair account bytes (offset 584).
    pub fn parse_lb_pair_internal_bitmap(data: &[u8]) -> Option<[u64; 16]> {
        if data.len() < LB_PAIR_BIN_ARRAY_BITMAP_OFFSET + LB_PAIR_BIN_ARRAY_BITMAP_WORDS * 8 {
            return None;
        }
        let mut words = [0u64; LB_PAIR_BIN_ARRAY_BITMAP_WORDS];
        for (i, word) in words
            .iter_mut()
            .enumerate()
            .take(LB_PAIR_BIN_ARRAY_BITMAP_WORDS)
        {
            let start = LB_PAIR_BIN_ARRAY_BITMAP_OFFSET + i * 8;
            *word = u64::from_le_bytes(data[start..start + 8].try_into().ok()?);
        }
        Some(words)
    }

    /// Serialize bitmap words for cache tags / JetStream metadata.
    pub fn format_bin_array_bitmap_tag(bitmap: &[u64; 16]) -> String {
        bitmap
            .iter()
            .map(|w| w.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Parse `bin_array_bitmap:<16 comma-separated u64>` tag.
    pub fn parse_bin_array_bitmap_tag(value: &str) -> Option<[u64; 16]> {
        let mut words = [0u64; LB_PAIR_BIN_ARRAY_BITMAP_WORDS];
        let parts: Vec<&str> = value.split(',').collect();
        if parts.len() != LB_PAIR_BIN_ARRAY_BITMAP_WORDS {
            return None;
        }
        for (i, p) in parts.iter().enumerate() {
            words[i] = p.trim().parse().ok()?;
        }
        Some(words)
    }

    pub fn parse_bitmap_extension_lb_pair(data: &[u8]) -> Option<Pubkey> {
        if data.len() < 40 {
            return None;
        }
        if data[0..8] != METEORA_BITMAP_EXTENSION_DISCRIMINATOR {
            return None;
        }
        Pubkey::try_from(&data[8..40]).ok()
    }

    /// Extract extension bitmap payload bytes `[40, 1576)`.
    pub fn parse_bitmap_extension_payload(data: &[u8]) -> Option<Vec<u8>> {
        if data.len() < METEORA_BITMAP_EXTENSION_ACCOUNT_SIZE {
            return None;
        }
        if data[0..8] != METEORA_BITMAP_EXTENSION_DISCRIMINATOR {
            return None;
        }
        Some(data[40..METEORA_BITMAP_EXTENSION_ACCOUNT_SIZE].to_vec())
    }

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

    pub fn internal_bitmap_has_bit(bitmap: &[u64; 16], array_index: i64) -> bool {
        if !(INTERNAL_BITMAP_INDEX_MIN..=INTERNAL_BITMAP_INDEX_MAX).contains(&array_index) {
            return false;
        }
        let bit_pos = (array_index + 512) as u32;
        let word = bit_pos / 64;
        let bit = bit_pos % 64;
        bitmap[word as usize] & (1u64 << bit) != 0
    }

    /// Whether any initialized bin-array index exists in the internal bitmap along `direction` from `start` (inclusive).
    pub fn internal_has_bit_in_direction(
        bitmap: &[u64; 16],
        start: i64,
        direction: SwapDirection,
    ) -> bool {
        match direction {
            SwapDirection::YtoX => {
                for idx in start..=INTERNAL_BITMAP_INDEX_MAX {
                    if Self::internal_bitmap_has_bit(bitmap, idx) {
                        return true;
                    }
                }
                false
            }
            SwapDirection::XtoY => {
                for idx in (INTERNAL_BITMAP_INDEX_MIN..=start).rev() {
                    if Self::internal_bitmap_has_bit(bitmap, idx) {
                        return true;
                    }
                }
                false
            }
        }
    }

    /// Geyser subscribe to bitmap-extension PDA when either swap direction lacks an internal bitmap bit from active index.
    pub fn needs_bitmap_extension_geyser_subscription(
        bitmap: &[u64; 16],
        active_array_index: i64,
    ) -> bool {
        !Self::internal_has_bit_in_direction(bitmap, active_array_index, SwapDirection::YtoX)
            || !Self::internal_has_bit_in_direction(bitmap, active_array_index, SwapDirection::XtoY)
    }

    fn extension_bitmap_has_bit(payload: &[u8], index: i64) -> bool {
        if payload.len() < METEORA_BITMAP_EXTENSION_PAYLOAD_LEN {
            return false;
        }
        let (positive_side, page, bit) = if index >= 0 {
            (true, index / 512 - 1, (index % 512) as u32)
        } else {
            let t = -(index + 1);
            (false, t / 512 - 1, (t % 512) as u32)
        };
        if !(0..12).contains(&page) {
            return false;
        }
        let base = if positive_side {
            page as usize * 64
        } else {
            768 + page as usize * 64
        };
        let limb = bit / 64;
        let b = bit % 64;
        let offset = base + limb as usize * 8;
        if offset + 8 > payload.len() {
            return false;
        }
        let word = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap_or([0; 8]));
        word & (1u64 << b) != 0
    }

    fn collect_extension_bin_array_indices(
        payload: &[u8],
        direction: SwapDirection,
        max: usize,
    ) -> Vec<i64> {
        let mut out = Vec::new();
        match direction {
            SwapDirection::YtoX => {
                let mut idx = 512i64;
                while out.len() < max && idx < 10_000 {
                    if Self::extension_bitmap_has_bit(payload, idx) {
                        out.push(idx);
                    }
                    idx += 1;
                }
            }
            SwapDirection::XtoY => {
                let mut idx = -513i64;
                while out.len() < max && idx > -10_000 {
                    if Self::extension_bitmap_has_bit(payload, idx) {
                        out.push(idx);
                    }
                    idx -= 1;
                }
            }
        }
        out
    }

    /// Resolve account #1 and bin-array remaining accounts from cached bitmap + optional extension payload.
    pub fn resolve_bitmap_swap_accounts(
        lb_pair: &Pubkey,
        bitmap: &[u64; 16],
        extension_payload: Option<&[u8]>,
        active_id: i32,
        direction: SwapDirection,
        bin_array_coverage: BinArrayCoverage,
    ) -> Result<(Pubkey, Vec<Pubkey>)> {
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let start = Self::bin_id_to_bin_array_index(active_id);

        if Self::internal_has_bit_in_direction(bitmap, start, direction) {
            let bin_arrays = Self::derive_bin_arrays_for_active_id_with_coverage(
                lb_pair,
                active_id,
                bin_array_coverage,
            )?;
            return Ok((program_id, bin_arrays));
        }

        let extension_pda = Self::derive_bitmap_extension_pda(lb_pair)?;
        let payload = extension_payload.ok_or_else(|| anyhow!("bitmap_extension_unknown"))?;
        let indices = Self::collect_extension_bin_array_indices(payload, direction, 3);
        if indices.is_empty() {
            return Err(anyhow!("bitmap_extension_unknown"));
        }
        let mut bin_arrays = Vec::with_capacity(indices.len());
        for idx in indices {
            bin_arrays.push(Self::derive_bin_array_pda(lb_pair, idx)?);
        }
        Ok((extension_pda, bin_arrays))
    }

    pub fn resolve_token_program_for_mint(
        mint: &Pubkey,
        extra_data_value: Option<&str>,
        pool_field: Option<Pubkey>,
    ) -> Result<Pubkey> {
        let tokenkeg = Pubkey::from_str(TOKEN_PROGRAM)?;
        let token_2022 = Pubkey::from_str(TOKEN_2022_PROGRAM)?;
        let wsol = Pubkey::from_str(WSOL_MINT)?;

        let is_valid = |p: &Pubkey| *p == tokenkeg || *p == token_2022;

        if let Some(s) = extra_data_value {
            let p = Pubkey::from_str(s)?;
            if is_valid(&p) {
                return Ok(p);
            }
            return Err(anyhow!("token_program_unknown"));
        }
        if let Some(p) = pool_field {
            if is_valid(&p) {
                return Ok(p);
            }
            return Err(anyhow!("token_program_unknown"));
        }
        if *mint == wsol {
            return Ok(tokenkeg);
        }
        Err(anyhow!("token_program_unknown"))
    }

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
            bin_arrays.push(Self::derive_bin_array_pda(lb_pair, index)?);
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
        bin_array_bitmap: Option<[u64; 16]>,
        bitmap_extension_payload: Option<&[u8]>,
        token_x_program: &Pubkey,
        token_y_program: &Pubkey,
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
            0,
            BinArrayCoverage::ActiveOnly,
            bin_array_bitmap,
            bitmap_extension_payload,
        )
    }

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
            None,
        )
    }

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
        bin_array_bitmap: Option<[u64; 16]>,
        bitmap_extension_payload: Option<&[u8]>,
    ) -> Result<Instruction> {
        ensure!(amount_in > 0, "Amount in must be positive");
        ensure!(min_amount_out > 0, "Min amount out must be positive");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;
        let token_2022_id = Pubkey::from_str(TOKEN_2022_PROGRAM)?;
        let use_swap2 = *token_x_program == token_2022_id || *token_y_program == token_2022_id;

        let bitmap = bin_array_bitmap.ok_or_else(|| anyhow!("bitmap_unknown"))?;

        let (bin_array_bitmap_extension, bin_arrays) = Self::resolve_bitmap_swap_accounts(
            lb_pair,
            &bitmap,
            bitmap_extension_payload,
            active_id,
            direction,
            bin_array_coverage,
        )?;

        let (oracle, _) = Pubkey::find_program_address(&[b"oracle", lb_pair.as_ref()], &program_id);
        let (event_authority, _) =
            Pubkey::find_program_address(&[b"__event_authority"], &program_id);

        let (user_token_in, user_token_out) = match direction {
            SwapDirection::XtoY => (user_token_x, user_token_y),
            SwapDirection::YtoX => (user_token_y, user_token_x),
        };

        let mut data = Vec::with_capacity(if use_swap2 { 28 } else { 24 });
        if use_swap2 {
            data.extend_from_slice(&SWAP2_DISCRIMINATOR);
        } else {
            data.extend_from_slice(&SWAP_DISCRIMINATOR);
        }
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());
        if use_swap2 {
            data.extend_from_slice(&[0u8; 4]);
        }

        let mut accounts = vec![
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
            AccountMeta::new_readonly(*token_x_program, false),
            AccountMeta::new_readonly(*token_y_program, false),
        ];

        if use_swap2 {
            let memo = Pubkey::from_str(MEMO_PROGRAM)?;
            accounts.push(AccountMeta::new_readonly(memo, false));
            accounts.push(AccountMeta::new_readonly(event_authority, false));
            accounts.push(AccountMeta::new_readonly(program_id, false));
        } else {
            accounts.push(AccountMeta::new_readonly(event_authority, false));
            accounts.push(AccountMeta::new_readonly(program_id, false));
        }

        for bin_array_pda in bin_arrays {
            accounts.push(AccountMeta::new(bin_array_pda, false));
        }

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

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

    pub fn bin_id_to_bin_array_index(bin_id: i32) -> i64 {
        const BIN_ARRAY_SIZE: i32 = 70;
        bin_id.div_euclid(BIN_ARRAY_SIZE) as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitmap_with_bit_at_index(array_index: i64) -> [u64; 16] {
        let mut b = [0u64; 16];
        let bit_pos = (array_index + 512) as u32;
        let word = bit_pos / 64;
        let bit = bit_pos % 64;
        b[word as usize] = 1u64 << bit;
        b
    }

    #[test]
    fn test_bin_array_index_calculation() {
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(0), 0);
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(69), 0);
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(70), 1);
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(-1), -1);
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(-70), -1);
        assert_eq!(MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(-71), -2);
    }

    #[test]
    fn test_derive_bin_array_pda() {
        let lb_pair = Pubkey::new_unique();
        let pda1 = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&lb_pair, 0).unwrap();
        let pda2 = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&lb_pair, 0).unwrap();
        assert_eq!(pda1, pda2);
        let pda3 = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&lb_pair, 1).unwrap();
        assert_ne!(pda1, pda3);
    }

    #[test]
    fn bitmap_extension_pda_uses_official_bitmap_seed() {
        let lb_pair = Pubkey::from_str("2TD1fMPg2w7Hjt8bASSdxi92YFNQFgvdznqVApe3NGpn").unwrap();
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM).unwrap();
        let (official, _) =
            Pubkey::find_program_address(&[b"bitmap", lb_pair.as_ref()], &program_id);
        let derived = MeteoraDlmmSwapBuilder::derive_bitmap_extension_pda(&lb_pair).unwrap();
        assert_eq!(derived, official);
    }

    #[test]
    fn swap_ytox_internal_bit_at_active_uses_program_id_legacy_swap() {
        let lb_pair = Pubkey::new_unique();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let builder = MeteoraDlmmSwapBuilder::new(rpc);
        let user = Pubkey::new_unique();
        let token_prog = Pubkey::from_str(TOKEN_PROGRAM).unwrap();
        let active_id = -244i32;
        let array_index = MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(active_id);
        assert_eq!(array_index, -4);
        let bitmap = bitmap_with_bit_at_index(array_index);

        let ix = builder
            .build_swap_with_bins_sync_coverage(
                &lb_pair,
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &token_prog,
                &token_prog,
                &user,
                1,
                1,
                SwapDirection::YtoX,
                active_id,
                10,
                BinArrayCoverage::AdjacentThree,
                Some(bitmap),
                None,
            )
            .expect("build");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM).unwrap();
        assert_eq!(ix.accounts[1].pubkey, program_id);
        assert_eq!(ix.data.len(), 24);
        assert_eq!(ix.data[0..8], SWAP_DISCRIMINATOR);
        assert!(!ix
            .accounts
            .iter()
            .any(|a| a.pubkey == Pubkey::from_str(MEMO_PROGRAM).unwrap()));
    }

    #[test]
    fn missing_bitmap_words_is_bitmap_unknown() {
        let lb_pair = Pubkey::new_unique();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let builder = MeteoraDlmmSwapBuilder::new(rpc);
        let err = builder
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
                &Pubkey::new_unique(),
                1,
                1,
                SwapDirection::YtoX,
                -244,
                10,
                BinArrayCoverage::AdjacentThree,
                None,
                None,
            )
            .unwrap_err();
        assert!(err.to_string().contains("bitmap_unknown"));
    }

    #[test]
    fn empty_internal_bitmap_uses_extension_pda_ytox_index_512() {
        let lb_pair = Pubkey::new_unique();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let builder = MeteoraDlmmSwapBuilder::new(rpc);
        let bitmap = [0u64; 16];
        let mut ext = vec![0u8; METEORA_BITMAP_EXTENSION_PAYLOAD_LEN];
        ext[0] = 1; // positive page 0 limb 0 bit 0 => index 512

        let ix = builder
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
                &Pubkey::new_unique(),
                1,
                1,
                SwapDirection::YtoX,
                -244,
                10,
                BinArrayCoverage::AdjacentThree,
                Some(bitmap),
                Some(&ext),
            )
            .expect("build");

        assert_eq!(
            ix.accounts[1].pubkey,
            MeteoraDlmmSwapBuilder::derive_bitmap_extension_pda(&lb_pair).unwrap()
        );
        let expected_first = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&lb_pair, 512).unwrap();
        assert_eq!(ix.accounts.last().unwrap().pubkey, expected_first);
    }

    #[test]
    fn empty_internal_bitmap_xtoy_extension_negative_513() {
        let lb_pair = Pubkey::new_unique();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let builder = MeteoraDlmmSwapBuilder::new(rpc);
        let bitmap = [0u64; 16];
        let mut ext = vec![0u8; METEORA_BITMAP_EXTENSION_PAYLOAD_LEN];
        ext[768] = 1; // negative page 0 limb 0 => index -513

        let ix = builder
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
                &Pubkey::new_unique(),
                1,
                1,
                SwapDirection::XtoY,
                100,
                10,
                BinArrayCoverage::AdjacentThree,
                Some(bitmap),
                Some(&ext),
            )
            .expect("build");

        assert_eq!(
            ix.accounts[1].pubkey,
            MeteoraDlmmSwapBuilder::derive_bitmap_extension_pda(&lb_pair).unwrap()
        );
        let expected_first = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&lb_pair, -513).unwrap();
        assert_eq!(ix.accounts.last().unwrap().pubkey, expected_first);
    }

    #[test]
    fn swap2_token2022_on_x_uses_memo_and_event_accounts() {
        let lb_pair = Pubkey::new_unique();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let builder = MeteoraDlmmSwapBuilder::new(rpc);
        let active_id = 70;
        let array_index = MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(active_id);
        let bitmap = bitmap_with_bit_at_index(array_index);
        let tokenkeg = Pubkey::from_str(TOKEN_PROGRAM).unwrap();
        let token2022 = Pubkey::from_str(TOKEN_2022_PROGRAM).unwrap();
        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM).unwrap();
        let (event_authority, _) =
            Pubkey::find_program_address(&[b"__event_authority"], &program_id);

        let ix = builder
            .build_swap_with_bins_sync_coverage(
                &lb_pair,
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &token2022,
                &tokenkeg,
                &Pubkey::new_unique(),
                1,
                1,
                SwapDirection::YtoX,
                active_id,
                10,
                BinArrayCoverage::ActiveOnly,
                Some(bitmap),
                None,
            )
            .expect("build");

        assert_eq!(ix.data.len(), 28);
        assert_eq!(ix.data[0..8], SWAP2_DISCRIMINATOR);
        assert_eq!(ix.data[24..28], [0, 0, 0, 0]);
        assert_eq!(
            ix.accounts[13].pubkey,
            Pubkey::from_str(MEMO_PROGRAM).unwrap()
        );
        assert_eq!(ix.accounts[14].pubkey, event_authority);
    }

    #[test]
    fn invalid_token_program_is_unknown() {
        let mint = Pubkey::new_unique();
        let err = MeteoraDlmmSwapBuilder::resolve_token_program_for_mint(
            &mint,
            Some(&Pubkey::new_unique().to_string()),
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("token_program_unknown"));
    }

    #[test]
    fn parse_meteora_lb_pair_real_bitmap_word_six() {
        let data = include_bytes!("../../../meteora_lb_pair_real.bin");
        let words = MeteoraDlmmSwapBuilder::parse_lb_pair_internal_bitmap(data).expect("bitmap");
        assert_eq!(words[6], 0xfffffffc00000000u64);
        let active_id = i32::from_le_bytes(data[0x4C..0x50].try_into().unwrap());
        assert_eq!(active_id, -4990);
        let array_index = MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(active_id);
        assert_eq!(array_index, -72);
        assert!(MeteoraDlmmSwapBuilder::internal_bitmap_has_bit(&words, -72));
    }
}
