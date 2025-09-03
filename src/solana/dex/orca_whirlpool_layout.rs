//! Orca Whirlpool Account Layout (Canonical Subset)
//!
//! This module defines (non-exhaustive) canonical byte offsets for the on-chain
//! Whirlpool account required for quoting & swap planning. Source: public
//! open‑source Orca Whirlpool program / IDL (simplified, only the needed subset).
//!
//! Layout (sequential after 8‑byte Anchor discriminator):
//!   0   .. 32  whirlpools_config (Pubkey)
//!   32  .. 33  bump (u8)
//!   33  .. 35  tick_spacing (u16 LE)
//!   35  .. 37  fee_rate (u16 LE, in hundredths of a bip? -> treat as raw bps)
//!   37  .. 39  protocol_fee_rate (u16 LE)
//!   39  .. 55  liquidity (u128 LE)
//!   55  .. 71  sqrt_price (u128 LE)
//!   71  .. 75  tick_current_index (i32 LE)
//!   75  .. 83  protocol_fee_owed_a (u64 LE)
//!   83  .. 91  protocol_fee_owed_b (u64 LE)
//!   91  .. 123 token_mint_a (Pubkey)
//!   123 .. 155 token_vault_a (Pubkey)
//!   155 .. 187 token_mint_b (Pubkey)
//!   187 .. 219 token_vault_b (Pubkey)
//!   219 .. 251 fee_tier (Pubkey)
//! (remaining bytes: reward infos, padding, etc. not parsed here)
//!
//! NOTE: Offsets include the 8‑byte discriminator, so actual absolute offsets
//! below already account for it.
//!
//! Safety: If the account is smaller than the minimum required span we return None.
//! If parsed values look invalid (zero pubkeys, identical mints) we also return None.

use solana_sdk::pubkey::Pubkey;

pub const MIN_WHIRLPOOL_ACCOUNT_LEN: usize = 251; // up to end of fee_tier

// Absolute offsets (already include discriminator region)
pub const OFF_CFG: usize = 8;               // whirlpools_config
pub const OFF_BUMP: usize = OFF_CFG + 32;   // u8
pub const OFF_TICK_SPACING: usize = OFF_BUMP + 1; // u16
pub const OFF_FEE_RATE: usize = OFF_TICK_SPACING + 2; // u16
pub const OFF_PROTOCOL_FEE_RATE: usize = OFF_FEE_RATE + 2; // u16
pub const OFF_LIQUIDITY: usize = OFF_PROTOCOL_FEE_RATE + 2; // u128
pub const OFF_SQRT_PRICE: usize = OFF_LIQUIDITY + 16;       // u128
pub const OFF_TICK_CURRENT: usize = OFF_SQRT_PRICE + 16;    // i32
pub const OFF_PROTOCOL_FEE_OWED_A: usize = OFF_TICK_CURRENT + 4; // u64
pub const OFF_PROTOCOL_FEE_OWED_B: usize = OFF_PROTOCOL_FEE_OWED_A + 8; // u64
pub const OFF_TOKEN_MINT_A: usize = OFF_PROTOCOL_FEE_OWED_B + 8; // Pubkey
pub const OFF_TOKEN_VAULT_A: usize = OFF_TOKEN_MINT_A + 32;      // Pubkey
pub const OFF_TOKEN_MINT_B: usize = OFF_TOKEN_VAULT_A + 32;      // Pubkey
pub const OFF_TOKEN_VAULT_B: usize = OFF_TOKEN_MINT_B + 32;      // Pubkey
pub const OFF_FEE_TIER: usize = OFF_TOKEN_VAULT_B + 32;          // Pubkey

#[derive(Debug, Clone)]
pub struct WhirlpoolParsed {
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    pub fee_tier: Pubkey,
    pub fee_rate: u16,
    pub protocol_fee_rate: u16,
    pub tick_spacing: u16,
    pub tick_current_index: i32,
    pub liquidity: u128,
    pub sqrt_price: u128,
}

pub fn parse_whirlpool(data: &[u8]) -> Option<WhirlpoolParsed> {
    if data.len() < MIN_WHIRLPOOL_ACCOUNT_LEN { return None; }
    let slice = |off: usize, len: usize| -> Option<&[u8]> { if off + len <= data.len() { Some(&data[off..off+len]) } else { None } };
    macro_rules! pk { ($o:expr) => {{ let b = slice($o,32)?; Pubkey::new_from_array(b.try_into().ok()?) }}; }
    let token_mint_a = pk!(OFF_TOKEN_MINT_A);
    let token_mint_b = pk!(OFF_TOKEN_MINT_B);
    if token_mint_a == token_mint_b || token_mint_a.to_bytes() == [0u8;32] || token_mint_b.to_bytes() == [0u8;32] { return None; }
    let token_vault_a = pk!(OFF_TOKEN_VAULT_A);
    let token_vault_b = pk!(OFF_TOKEN_VAULT_B);
    if token_vault_a == token_vault_b || token_vault_a.to_bytes() == [0u8;32] || token_vault_b.to_bytes() == [0u8;32] { return None; }
    let fee_tier = pk!(OFF_FEE_TIER);
    let u16_le = |o: usize| -> Option<u16> { let b = slice(o,2)?; Some(u16::from_le_bytes([b[0], b[1]])) };
    let i32_le = |o: usize| -> Option<i32> { let b = slice(o,4)?; Some(i32::from_le_bytes([b[0], b[1], b[2], b[3]])) };
    let u128_le = |o: usize| -> Option<u128> { let b = slice(o,16)?; Some(u128::from_le_bytes(b.try_into().ok()?)) };
    let fee_rate = u16_le(OFF_FEE_RATE)?; if fee_rate == 0 || fee_rate > 10_000 { return None; }
    let protocol_fee_rate = u16_le(OFF_PROTOCOL_FEE_RATE)?;
    let tick_spacing = u16_le(OFF_TICK_SPACING)?; if tick_spacing == 0 { return None; }
    let tick_current_index = i32_le(OFF_TICK_CURRENT)?;
    let liquidity = u128_le(OFF_LIQUIDITY)?;
    let sqrt_price = u128_le(OFF_SQRT_PRICE)?;
    Some(WhirlpoolParsed { token_mint_a, token_mint_b, token_vault_a, token_vault_b, fee_tier, fee_rate, protocol_fee_rate, tick_spacing, tick_current_index, liquidity, sqrt_price })
}

/// (Legacy) simple sanity helper retained for external callers needing quick validation.
pub fn sanity_mints_vaults(ma: &Pubkey, va: &Pubkey, mb: &Pubkey, vb: &Pubkey) -> bool {
    ma != mb && va != vb && ma.to_bytes() != [0u8;32] && mb.to_bytes() != [0u8;32] && va.to_bytes() != [0u8;32] && vb.to_bytes() != [0u8;32]
}

