//! Orca Whirlpool classic `TickArray` PDA helpers and account parse (Job 1 library).
//!
//! PDA seeds match on-chain Anchor: decimal ASCII `start_tick_index`, not `i32::to_le_bytes()`.

use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

use super::orca::ORCA_WHIRLPOOL_PROGRAM;

pub const TICK_ARRAY_SIZE: i32 = 88;

/// Classic Anchor `TickArray` account discriminator (`account:TickArray`).
pub const TICK_ARRAY_DISCRIMINATOR: [u8; 8] = [0x45, 0x61, 0xbd, 0xbe, 0x6e, 0x07, 0x42, 0xbb];

const TICK_ACCOUNT_BODY_LEN: usize = 4 + (Tick::LEN * TICK_ARRAY_SIZE as usize) + 32;
pub const MIN_TICK_ARRAY_ACCOUNT_LEN: usize = 8 + TICK_ACCOUNT_BODY_LEN;

const TICK_LEN: usize = Tick::LEN;

struct Tick;

impl Tick {
    const LEN: usize = 113;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTick {
    pub tick_index: i32,
    pub initialized: bool,
    pub liquidity_net: i128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTickArray {
    pub whirlpool: Pubkey,
    pub start_tick_index: i32,
    pub ticks: Vec<ParsedTick>,
}

/// First tick index of the tick array that contains `tick_index`.
pub fn get_tick_array_start_index(tick_index: i32, tick_spacing: i32) -> i32 {
    let ts = tick_spacing;
    let real_index = tick_index.div_euclid(ts).div_euclid(TICK_ARRAY_SIZE);
    real_index * ts * TICK_ARRAY_SIZE
}

pub fn derive_tick_array_pda(pool: &Pubkey, start_tick_index: i32) -> Pubkey {
    let start_tick_str = start_tick_index.to_string();
    let seeds: &[&[u8]] = &[b"tick_array", pool.as_ref(), start_tick_str.as_bytes()];
    Pubkey::find_program_address(
        seeds,
        &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).expect("orca program id"),
    )
    .0
}

/// Planned quote window: current array start ±2 (`s0-2w … s0+2w`, `w = tick_spacing * 88`).
pub fn planned_orca_tick_array_pubkeys(
    pool: &Pubkey,
    tick_now: i32,
    tick_spacing: i32,
) -> [Pubkey; 5] {
    let w = tick_spacing * TICK_ARRAY_SIZE;
    let s0 = get_tick_array_start_index(tick_now, tick_spacing);
    [
        derive_tick_array_pda(pool, s0 - 2 * w),
        derive_tick_array_pda(pool, s0 - w),
        derive_tick_array_pda(pool, s0),
        derive_tick_array_pda(pool, s0 + w),
        derive_tick_array_pda(pool, s0 + 2 * w),
    ]
}

/// Swap TX uses three consecutive arrays in swap direction (same starts as `orca.rs`).
pub fn swap_direction_tick_array_starts(
    tick_now: i32,
    tick_spacing: i32,
    a_to_b: bool,
) -> (i32, i32, i32) {
    let ticks_per_array = tick_spacing * TICK_ARRAY_SIZE;
    let s0 = get_tick_array_start_index(tick_now, tick_spacing);
    let (s1, s2) = if a_to_b {
        let s_prev = s0 - ticks_per_array;
        (s_prev, s_prev - ticks_per_array)
    } else {
        let s_next = s0 + ticks_per_array;
        (s_next, s_next + ticks_per_array)
    };
    (s0, s1, s2)
}

pub fn parse_tick_array(data: &[u8]) -> Option<ParsedTickArray> {
    if data.len() < MIN_TICK_ARRAY_ACCOUNT_LEN {
        return None;
    }
    if data[0..8] != TICK_ARRAY_DISCRIMINATOR {
        return None;
    }
    let body = &data[8..];
    let start_tick_index = i32::from_le_bytes(body[0..4].try_into().ok()?);
    let mut offset = 4usize;
    let mut ticks = Vec::with_capacity(TICK_ARRAY_SIZE as usize);
    for i in 0..TICK_ARRAY_SIZE as usize {
        // Absolute tick index requires pool `tick_spacing`; walker resolves via `tick_index_at_slot`.
        let tick_index = start_tick_index + i as i32;
        let tick_slice = body.get(offset..offset + TICK_LEN)?;
        let parsed = parse_tick_bytes(tick_index, tick_slice)?;
        ticks.push(parsed);
        offset += TICK_LEN;
    }
    let whirlpool = Pubkey::new_from_array(body[offset..offset + 32].try_into().ok()?);
    if offset + 32 != body.len() {
        return None;
    }
    Some(ParsedTickArray {
        whirlpool,
        start_tick_index,
        ticks,
    })
}

/// Usable on-chain tick index for a slot in this array.
pub fn tick_index_at_slot(start_tick_index: i32, slot: usize, tick_spacing: u16) -> i32 {
    start_tick_index + (slot as i32) * tick_spacing as i32
}

fn parse_tick_bytes(tick_index: i32, data: &[u8]) -> Option<ParsedTick> {
    if data.len() < TICK_LEN {
        return None;
    }
    let initialized = data[0] != 0;
    let liquidity_net = i128::from_le_bytes(data[1..17].try_into().ok()?);
    Some(ParsedTick {
        tick_index,
        initialized,
        liquidity_net,
    })
}

/// Build classic TickArray account bytes (8-byte discriminator + body) for tests.
pub fn build_tick_array_account_bytes(
    start_tick_index: i32,
    whirlpool: Pubkey,
    tick_spacing: u16,
    tick_updates: &[(i32, bool, i128)],
) -> Vec<u8> {
    let mut body = vec![0u8; TICK_ACCOUNT_BODY_LEN];
    body[0..4].copy_from_slice(&start_tick_index.to_le_bytes());
    let mut offset = 4usize;
    for slot in 0..TICK_ARRAY_SIZE as usize {
        let tick_index = start_tick_index + (slot as i32) * tick_spacing as i32;
        let mut tick_data = [0u8; TICK_LEN];
        if let Some((_, init, liq_net)) = tick_updates.iter().find(|(idx, _, _)| *idx == tick_index)
        {
            tick_data[0] = u8::from(*init);
            tick_data[1..17].copy_from_slice(&liq_net.to_le_bytes());
        }
        body[offset..offset + TICK_LEN].copy_from_slice(&tick_data);
        offset += TICK_LEN;
    }
    body[offset..offset + 32].copy_from_slice(whirlpool.as_ref());
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&TICK_ARRAY_DISCRIMINATOR);
    out.extend_from_slice(&body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_roundtrip_start_and_liquidity_net() {
        let pool = Pubkey::new_unique();
        let start = -704i32;
        let spacing = 8u16;
        let tick_idx = -64i32;
        let liq_net = 1_234_567i128;
        let bytes =
            build_tick_array_account_bytes(start, pool, spacing, &[(tick_idx, true, liq_net)]);
        let parsed = parse_tick_array(&bytes).expect("parse");
        assert_eq!(parsed.start_tick_index, start);
        assert_eq!(parsed.whirlpool, pool);
        assert_eq!(parsed.ticks.len(), TICK_ARRAY_SIZE as usize);
        let slot = ((tick_idx - start) / spacing as i32) as usize;
        assert!(parsed.ticks[slot].initialized);
        assert_eq!(parsed.ticks[slot].liquidity_net, liq_net);
    }

    #[test]
    fn parse_rejects_short_and_wrong_discriminator() {
        let pool = Pubkey::new_unique();
        let good = build_tick_array_account_bytes(0, pool, 8, &[]);
        assert!(parse_tick_array(&good[..good.len() - 1]).is_none());
        let mut bad = good.clone();
        bad[0] ^= 0xff;
        assert!(parse_tick_array(&bad).is_none());
    }

    #[test]
    fn planned_five_contains_swap_three_a_to_b_and_b_to_a() {
        let pool = Pubkey::new_unique();
        let spacing = 64;
        let tick_now = 2800;
        let planned = planned_orca_tick_array_pubkeys(&pool, tick_now, spacing);
        for a_to_b in [true, false] {
            let (s0, s1, s2) = swap_direction_tick_array_starts(tick_now, spacing, a_to_b);
            let pdas = [
                derive_tick_array_pda(&pool, s0),
                derive_tick_array_pda(&pool, s1),
                derive_tick_array_pda(&pool, s2),
            ];
            for pda in pdas {
                assert!(
                    planned.contains(&pda),
                    "TX PDA must be in planned window (a_to_b={a_to_b})"
                );
            }
        }
    }
}
