//! Orca Whirlpool Account Layout (Stub)
//! 
//! This module centralizes (placeholder) offsets for the Whirlpool on-chain account.
//! A proper implementation should source constants from the official Orca Whirlpool
//! program (IDL / SDK). For now we keep heuristic fallbacks plus a candidate list.
//! 
//! Fields we care about (initial minimal set):
//! - token_mint_a
//! - token_vault_a
//! - token_mint_b
//! - token_vault_b
//! - fee_tier (bps or structured tier reference)
//! 
//! Once verified, set `PRIMARY_OFFSETS` to the canonical tuple and optionally
//! remove redundant candidates.
use solana_sdk::pubkey::Pubkey;

/// Candidate layout tuples (mint_a, vault_a, mint_b, vault_b) byte offsets.
/// Ordered by confidence (first = most probable).
pub const CANDIDATE_OFFSETS: &[(usize, usize, usize, usize)] = &[
    // Ordered candidate guesses (update with real constants when confirmed)
    (128, 160, 192, 224),
    (164, 196, 228, 260),
    (176, 208, 240, 272),
];

/// Fee scan search regions (start, len) – we look for a plausible u16 fee value (1..=1000 bps).
pub const FEE_SCAN_HEAD: (usize, usize) = (0, 160);
pub const FEE_SCAN_TAIL_LEN: usize = 128; // last N bytes

/// Validate a potential mint / vault tuple (basic sanity checks).
pub fn sanity_mints_vaults(ma: &Pubkey, va: &Pubkey, mb: &Pubkey, vb: &Pubkey) -> bool {
    ma != mb && va != vb && ma.to_bytes() != [0u8;32] && mb.to_bytes() != [0u8;32] && va.to_bytes() != [0u8;32] && vb.to_bytes() != [0u8;32]
}

/// Heuristic fee extraction: scans a segment for first plausible u16 (1..=1000).
pub fn scan_fee_bps(segment: &[u8]) -> Option<u32> {
    for i in (0..segment.len().saturating_sub(1)).step_by(2) {
        let val = u16::from_le_bytes([segment[i], segment[i+1]]) as u32;
        if (1..=1000).contains(&val) { return Some(val); }
    }
    None
}
