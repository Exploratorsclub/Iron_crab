//! Orca Whirlpool Account Layout (Canonical Subset)
//!
//! This module defines (non-exhaustive) canonical byte offsets for the on-chain
//! Whirlpool account required for quoting & swap planning. Source: public
//! open‑source Orca Whirlpool program / IDL (simplified, only the needed subset).
//!
//! Layout (sequential, includes 8‑byte Anchor discriminator):
//!   0   .. 8   discriminator (Anchor)
//!   8   .. 40  whirlpools_config (Pubkey)
//!   40  .. 41  whirlpool_bump (u8)
//!   41  .. 43  tick_spacing (u16 LE)
//!   43  .. 45  fee_tier_index_seed (u8[2])
//!   45  .. 47  fee_rate (u16 LE, in bips)
//!   47  .. 49  protocol_fee_rate (u16 LE)
//!   49  .. 65  liquidity (u128 LE)
//!   65  .. 81  sqrt_price (u128 LE)
//!   81  .. 85  tick_current_index (i32 LE)
//!   85  .. 93  protocol_fee_owed_a (u64 LE)
//!   93  .. 101 protocol_fee_owed_b (u64 LE)
//!   101 .. 133 token_mint_a (Pubkey)
//!   133 .. 165 token_vault_a (Pubkey)
//!   165 .. 181 fee_growth_global_a (u128 LE)
//!   181 .. 213 token_mint_b (Pubkey)
//!   213 .. 245 token_vault_b (Pubkey)
//!   245 .. 261 fee_growth_global_b (u128 LE)
//!   261 .. 269 reward_last_updated_timestamp (u64 LE)
//!   269 .. 653 reward_infos[3] (128 bytes each, not parsed here)
//!
//! Reference: https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/state/whirlpool.rs#L36-L54

use solana_sdk::pubkey::Pubkey;

pub const MIN_WHIRLPOOL_ACCOUNT_LEN: usize = 245; // up to end of token_vault_b

// Absolute offsets
pub const OFF_DISCRIMINATOR: usize = 0; // 8 bytes
pub const OFF_CONFIG: usize = 8; // whirlpools_config
pub const OFF_BUMP: usize = 40; // whirlpool_bump (u8)
pub const OFF_TICK_SPACING: usize = 41; // u16
pub const OFF_FEE_TIER_INDEX_SEED: usize = 43; // u8[2]
pub const OFF_FEE_RATE: usize = 45; // u16
pub const OFF_PROTOCOL_FEE_RATE: usize = 47; // u16
pub const OFF_LIQUIDITY: usize = 49; // u128
pub const OFF_SQRT_PRICE: usize = 65; // u128
pub const OFF_TICK_CURRENT: usize = 81; // i32
pub const OFF_PROTOCOL_FEE_OWED_A: usize = 85; // u64
pub const OFF_PROTOCOL_FEE_OWED_B: usize = 93; // u64
pub const OFF_TOKEN_MINT_A: usize = 101; // Pubkey
pub const OFF_TOKEN_VAULT_A: usize = 133; // Pubkey
pub const OFF_FEE_GROWTH_GLOBAL_A: usize = 165; // u128
pub const OFF_TOKEN_MINT_B: usize = 181; // Pubkey
pub const OFF_TOKEN_VAULT_B: usize = 213; // Pubkey
pub const OFF_FEE_GROWTH_GLOBAL_B: usize = 245; // u128

#[derive(Debug, Clone)]
pub struct WhirlpoolParsed {
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    pub fee_rate: u16,
    pub protocol_fee_rate: u16,
    pub tick_spacing: u16,
    pub tick_current_index: i32,
    pub liquidity: u128,
    pub sqrt_price: u128,
}

pub fn parse_whirlpool(data: &[u8]) -> Option<WhirlpoolParsed> {
    if data.len() < MIN_WHIRLPOOL_ACCOUNT_LEN {
        return None;
    }
    let slice = |off: usize, len: usize| -> Option<&[u8]> {
        if off + len <= data.len() {
            Some(&data[off..off + len])
        } else {
            None
        }
    };
    macro_rules! pk {
        ($o:expr) => {{
            let b = slice($o, 32)?;
            Pubkey::new_from_array(b.try_into().ok()?)
        }};
    }
    let token_mint_a = pk!(OFF_TOKEN_MINT_A);
    let token_mint_b = pk!(OFF_TOKEN_MINT_B);
    if token_mint_a == token_mint_b
        || token_mint_a.to_bytes() == [0u8; 32]
        || token_mint_b.to_bytes() == [0u8; 32]
    {
        return None;
    }
    let token_vault_a = pk!(OFF_TOKEN_VAULT_A);
    let token_vault_b = pk!(OFF_TOKEN_VAULT_B);
    if token_vault_a == token_vault_b
        || token_vault_a.to_bytes() == [0u8; 32]
        || token_vault_b.to_bytes() == [0u8; 32]
    {
        return None;
    }
    let u16_le = |o: usize| -> Option<u16> {
        let b = slice(o, 2)?;
        Some(u16::from_le_bytes([b[0], b[1]]))
    };
    let i32_le = |o: usize| -> Option<i32> {
        let b = slice(o, 4)?;
        Some(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    };
    let u128_le = |o: usize| -> Option<u128> {
        let b = slice(o, 16)?;
        Some(u128::from_le_bytes(b.try_into().ok()?))
    };
    let fee_rate = u16_le(OFF_FEE_RATE)?;
    if fee_rate == 0 || fee_rate > 10_000 {
        return None;
    }
    let protocol_fee_rate = u16_le(OFF_PROTOCOL_FEE_RATE)?;
    let tick_spacing = u16_le(OFF_TICK_SPACING)?;
    if tick_spacing == 0 {
        return None;
    }
    let tick_current_index = i32_le(OFF_TICK_CURRENT)?;
    let liquidity = u128_le(OFF_LIQUIDITY)?;
    let sqrt_price = u128_le(OFF_SQRT_PRICE)?;
    Some(WhirlpoolParsed {
        token_mint_a,
        token_mint_b,
        token_vault_a,
        token_vault_b,
        fee_rate,
        protocol_fee_rate,
        tick_spacing,
        tick_current_index,
        liquidity,
        sqrt_price,
    })
}

/// (Legacy) simple sanity helper retained for external callers needing quick validation.
pub fn sanity_mints_vaults(ma: &Pubkey, va: &Pubkey, mb: &Pubkey, vb: &Pubkey) -> bool {
    ma != mb
        && va != vb
        && ma.to_bytes() != [0u8; 32]
        && mb.to_bytes() != [0u8; 32]
        && va.to_bytes() != [0u8; 32]
        && vb.to_bytes() != [0u8; 32]
}

// Allowed tick spacings observed in production (used for stricter validation)
const ALLOWED_TICK_SPACINGS: [u16; 8] = [1, 8, 16, 32, 64, 128, 256, 512];

/// Convert sqrt_price (Q64.64) to an approximate tick value (Uniswap v3 style)
/// tick ~= log_{1.0001}(price) where price = (sqrt_price^2 / 2^128).
fn approximate_tick(sqrt_price_q64: u128) -> Option<i32> {
    if sqrt_price_q64 == 0 {
        return None;
    }
    // Avoid overflow: cast to f64 progressively.
    let sp = sqrt_price_q64 as f64;
    // sqrt_price is scaled by 2^64
    let scale = (1u128 << 64) as f64;
    let ratio = sp / scale; // sqrt(price)
    if ratio <= 0.0 {
        return None;
    }
    let price = ratio * ratio;
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    // ln(price)/ln(1.0001)
    let tick = (price.ln() / 0.0001f64.ln()).round();
    if tick.abs() > 10_000_000.0 {
        return None;
    }
    Some(tick as i32)
}

/// Strict parser with deeper structural & semantic validation.
/// Centralizes safety checks so callers (e.g. refresh_pools) don't need to duplicate them.
pub fn parse_whirlpool_strict(data: &[u8]) -> Option<WhirlpoolParsed> {
    let p = parse_whirlpool(data)?;
    // Tick spacing must be in allowed set.
    if !ALLOWED_TICK_SPACINGS.contains(&p.tick_spacing) {
        return None;
    }
    // Basic liquidity / sqrt_price sanity.
    if p.liquidity == 0 || p.sqrt_price == 0 {
        return None;
    }
    // fee_rate: filter out extreme tiers (keep 1..=1000 typical)
    if p.fee_rate == 0 || p.fee_rate > 1000 {
        return None;
    }
    // Vaults should not equal mints (token account vs mint account distinction heuristic)
    if p.token_vault_a == p.token_mint_a || p.token_vault_b == p.token_mint_b {
        return None;
    }
    // Tick plausibility: ensure current tick aligns with sqrt_price within a tolerance.
    if let Some(approx_tick) = approximate_tick(p.sqrt_price) {
        let delta = (approx_tick - p.tick_current_index).abs();
        // Allow generous tolerance (some pools may have moved slightly since snapshot), but reject huge divergence.
        if delta as u32 > (p.tick_spacing as u32 * 50).max(500) {
            return None;
        }
    } else {
        return None;
    }
    // Additional bound on tick_current_index absolute magnitude.
    if p.tick_current_index.abs() > 5_000_000 {
        return None;
    }
    Some(p)
}
