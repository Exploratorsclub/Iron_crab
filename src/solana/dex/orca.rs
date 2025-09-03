
//! Orca Connector – Skeleton (Whirlpool/Classic)

use std::sync::Arc;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::str::FromStr;

use crate::solana::rpc::SolanaRpc;
use super::{Dex, Quote};
use solana_sdk::instruction::Instruction;

pub const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicV3QDeqAD9nukHf8stYwh5GozfX6rS3SAm"; // verify
// Approximate Whirlpool account size (may vary by version). Adjust if mismatch logged repeatedly.
pub const WHIRLPOOL_ACCOUNT_MIN_SIZE: usize = 400; // conservative lower bound
pub const WHIRLPOOL_ACCOUNT_MAX_SIZE: usize = 800; // safety upper bound

use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use super::orca_whirlpool_layout as layout;

#[derive(Clone, Debug)]
struct OrcaPool {
    base_mint: Pubkey,
    quote_mint: Pubkey,
    reserve_base: u128,
    reserve_quote: u128,
    fee_bps: u32,
    fee_tier: Option<Pubkey>,
    tick_spacing: Option<u16>,
    vault_a: Pubkey,
    vault_b: Pubkey,
    tick_current_index: Option<i32>,
}

pub struct Orca {
    rpc: Arc<SolanaRpc>,
    pools: Arc<DashMap<Pubkey, OrcaPool>>, // keyed by a pseudo pool id (mint xor) for now
    fee_tiers: Arc<DashMap<Pubkey, (u32, u16)>>,
}

impl Orca {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self { Self { rpc, pools: Arc::new(DashMap::new()), fee_tiers: Arc::new(DashMap::new()) } }

    fn find_pool(&self, input: &Pubkey, output: &Pubkey) -> Option<(Pubkey, bool, OrcaPool)> {
        for p in self.pools.iter() {
            let forward = p.base_mint == *input && p.quote_mint == *output;
            let reverse = p.base_mint == *output && p.quote_mint == *input;
            if forward || reverse { return Some((*p.key(), forward, p.clone())); }
        }
        None
    }

    pub fn insert_mock_pool(&self, base: Pubkey, quote: Pubkey, reserve_base: u128, reserve_quote: u128, fee_bps: u32) {
        self.pools.insert(base, OrcaPool { base_mint: base, quote_mint: quote, reserve_base, reserve_quote, fee_bps, fee_tier: None, tick_spacing: None, vault_a: Pubkey::new_unique(), vault_b: Pubkey::new_unique(), tick_current_index: None });
    }
}

#[async_trait]
impl Dex for Orca {
    async fn refresh_pools(&self) -> Result<()> {
    use solana_client::rpc_config::{RpcProgramAccountsConfig, RpcAccountInfoConfig};
    use solana_client::rpc_filter::RpcFilterType;
    use std::str::FromStr;
        use solana_sdk::pubkey::Pubkey;
        tracing::trace!("orca.refresh_pools() whirlpool fetch");
    let program_id = Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).map_err(|_| anyhow!("invalid whirlpool program id"))?;
    // DataSize filter (broad window) to reduce traffic
    let size_filter = RpcFilterType::DataSize(WHIRLPOOL_ACCOUNT_MAX_SIZE as u64);
    let filters = Some(vec![size_filter]);
        let acc_cfg = RpcAccountInfoConfig { encoding: None, data_slice: None, commitment: None, min_context_slot: None };
    let cfg = RpcProgramAccountsConfig { filters, account_config: acc_cfg, with_context: None, sort_results: None };
        let accounts = self.rpc.rpc.get_program_accounts_with_config(&program_id, cfg).await?;
    let mut added = 0u32;
    let mut fee_tier_keys: Vec<Pubkey> = Vec::new();
        for (addr, acc) in accounts.into_iter().take(5000) { // safety limit
            // Heuristic: minimal length check
            if acc.data.len() < WHIRLPOOL_ACCOUNT_MIN_SIZE || acc.data.len() > WHIRLPOOL_ACCOUNT_MAX_SIZE { continue; }
            if let Some(pool) = Self::decode_whirlpool_stub(addr, &acc.data) {
                // fetch vault balances
                let mut reserves = (0u128, 0u128);
                if let Ok(vaults) = self.rpc.rpc.get_multiple_accounts(&[pool.1, pool.2]).await {
                    if let Some(Some(v1)) = vaults.get(0).map(|o| o.as_ref()) { if v1.data.len() >= 72 { reserves.0 = Self::parse_token_amount(&v1.data) as u128; } }
                    if let Some(Some(v2)) = vaults.get(1).map(|o| o.as_ref()) { if v2.data.len() >= 72 { reserves.1 = Self::parse_token_amount(&v2.data) as u128; } }
                }
                let meta = pool.3;
                if let Some(ft) = meta.fee_tier_key { fee_tier_keys.push(ft); }
                let fee_bps = meta.fee_bps;
                let base_mint = pool.0.0; let quote_mint = pool.0.1;
                let id = addr; // use whirlpool account key as pool id
                self.pools.insert(id, OrcaPool { base_mint, quote_mint, reserve_base: reserves.0, reserve_quote: reserves.1, fee_bps, fee_tier: meta.fee_tier_key, tick_spacing: meta.tick_spacing, vault_a: pool.1, vault_b: pool.2, tick_current_index: meta.tick_current_index });
                added += 1;
            }
        }
        // Deduplicate & fetch fee tier accounts
        fee_tier_keys.sort_unstable(); fee_tier_keys.dedup();
        if !fee_tier_keys.is_empty() {
            if let Ok(accts) = self.rpc.rpc.get_multiple_accounts(&fee_tier_keys).await {
                for (i, acc_opt) in accts.into_iter().enumerate() {
                    if let Some(acc) = acc_opt { if acc.data.len() >= 4 {
                        let tick = u16::from_le_bytes([acc.data[0], acc.data[1]]);
                        let fee = u16::from_le_bytes([acc.data[2], acc.data[3]]) as u32;
                        if (1..=1000).contains(&fee) { self.fee_tiers.insert(fee_tier_keys[i], (fee, tick)); }
                    } }
                }
            }
            // Patch pools with authoritative values
            for mut p in self.pools.iter_mut() { if let Some(key) = p.fee_tier { if let Some((fee, tick)) = self.fee_tiers.get(&key).map(|v| *v) { p.fee_bps = fee; p.tick_spacing = Some(tick); } } }
        }
        tracing::info!(added, total = self.pools.len(), "orca.refresh_pools() done");
        Ok(())
    }

    async fn quote_exact_in(&self, _input_mint: &str, _output_mint: &str, _amount_in: u64) -> Result<Option<Quote>> {
        use std::str::FromStr;
        let input = Pubkey::from_str(_input_mint).map_err(|_| anyhow!("bad input mint"))?;
        let output = Pubkey::from_str(_output_mint).map_err(|_| anyhow!("bad output mint"))?;
        let pool = match self.find_pool(&input, &output) { Some(p) => p, None => return Ok(None) };
        let (pid, forward, p) = pool;
        let (rin, rout) = if forward { (p.reserve_base, p.reserve_quote) } else { (p.reserve_quote, p.reserve_base) };
        if rin == 0 || rout == 0 { return Ok(None); }
        let amount_in_u = _amount_in as u128;
        let fee_bps_u = p.fee_bps as u128;
        let amount_less_fee = amount_in_u * (10_000 - fee_bps_u) / 10_000;
        let out = (amount_less_fee * rout) / (rin + amount_less_fee);
        if out == 0 { return Ok(None); }
        let impact_bps = ((amount_less_fee * 10_000) / (rin + amount_less_fee)) as u32;
    Ok(Some(Quote { amount_out: out as u64, price_impact_bps: impact_bps, route: vec![pid.to_string()], fee_bps: p.fee_bps, in_reserve: rin, out_reserve: rout, input_mint: (if forward { input } else { output }).to_string(), output_mint: (if forward { output } else { input }).to_string() }))
    }

    fn build_swap_ix(&self, _input_mint: &str, _output_mint: &str, _amount_in: u64, _min_out: u64) -> Result<Vec<Instruction>> {
        use std::str::FromStr;
        use solana_sdk::instruction::AccountMeta as AM;
        let in_pk = Pubkey::from_str(_input_mint).map_err(|_| anyhow!("bad input mint"))?;
        let out_pk = Pubkey::from_str(_output_mint).map_err(|_| anyhow!("bad output mint"))?;
        let (pool_id, forward, pool) = self.find_pool(&in_pk, &out_pk).ok_or_else(|| anyhow!("no orca pool for pair"))?;
        let spacing = pool.tick_spacing.unwrap_or(64) as i32;
        let tick_now = pool.tick_current_index.unwrap_or(0);
        let start0 = align_to_start(tick_now, spacing);
        let start1 = align_to_start(tick_now + spacing * 88, spacing);
        let start_1 = align_to_start(tick_now - spacing * 88, spacing);
        let tick_arrays = [start_1, start0, start1].map(|s| derive_tick_array_pda(&pool_id, s));
        let oracle = derive_oracle_pda(&pool_id);
        let mut data = Vec::with_capacity(1 + 8 + 8 + 1 + 16);
        data.push(0u8);
        data.extend_from_slice(&_amount_in.to_le_bytes());
        data.extend_from_slice(&_min_out.to_le_bytes());
        data.push(if forward { 1 } else { 0 });
        data.extend_from_slice(&0u128.to_le_bytes());
        let program_id = Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).map_err(|_| anyhow!("orca program id"))?;
        let mut accounts = vec![
            AM { pubkey: pool_id, is_signer: false, is_writable: true },
            AM { pubkey: pool.vault_a, is_signer: false, is_writable: true },
            AM { pubkey: pool.vault_b, is_signer: false, is_writable: true },
        ];
        if let Some(ft) = pool.fee_tier { accounts.push(AM { pubkey: ft, is_signer: false, is_writable: false }); }
        for t in &tick_arrays { accounts.push(AM { pubkey: *t, is_signer: false, is_writable: true }); }
        accounts.push(AM { pubkey: oracle, is_signer: false, is_writable: false });
        // Placeholder user authority & token accounts (to be supplied by caller later)
        accounts.push(AM { pubkey: Pubkey::default(), is_signer: true, is_writable: false });
        accounts.push(AM { pubkey: Pubkey::default(), is_signer: false, is_writable: true });
        accounts.push(AM { pubkey: Pubkey::default(), is_signer: false, is_writable: true });
        Ok(vec![Instruction { program_id, accounts, data }])
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        self.pools.iter().map(|p| (p.base_mint.to_string(), p.quote_mint.to_string())).collect()
    }
}

impl Orca {
    // Very rough stub decode: attempts to extract token mints & vaults at plausible offsets.
    // THIS IS NOT A COMPLETE WHIRLPOOL DECODE. Replace with full layout as needed.
    fn decode_whirlpool_stub(_addr: Pubkey, data: &[u8]) -> Option<((Pubkey, Pubkey), Pubkey, Pubkey, WhirlpoolMeta)> {
        // Use centralized candidate offsets from layout module.
        const CANDIDATES: &[(usize, usize, usize, usize)] = layout::CANDIDATE_OFFSETS;
    let mut fee_bps_detected = 30u32;
    let mut tick_spacing_detected: Option<u16> = None;
    let mut fee_tier_key: Option<Pubkey> = None;
    let mut tick_current_index: Option<i32> = None;
        // Try pull a dedicated fee field candidate: search last 96 bytes for plausible u16 fee if not found earlier.
    for (ma, va, mb, vb) in CANDIDATES {
            let need = *vb + 32;
            if data.len() < need { continue; }
            let pk = |o: usize| -> Pubkey { Pubkey::new_from_array(data[o..o+32].try_into().unwrap()) };
            let mint_a = pk(*ma);
            let vault_a = pk(*va);
            let mint_b = pk(*mb);
            let vault_b = pk(*vb);
            // Basic sanity: mints not identical, vaults distinct, not default.
            if layout::sanity_mints_vaults(&mint_a, &vault_a, &mint_b, &vault_b) {
                // Fee: head scan then tail scan
                if let Some(f) = layout::scan_fee_bps(&data[layout::FEE_SCAN_HEAD.0..layout::FEE_SCAN_HEAD.1.min(data.len())]) { fee_bps_detected = f; }
                if fee_bps_detected == 30 { // fallback tail
                    let start_tail = data.len().saturating_sub(layout::FEE_SCAN_TAIL_LEN);
                    if let Some(f) = layout::scan_fee_bps(&data[start_tail..]) { fee_bps_detected = f; }
                }
        // Attempt fee tier meta decode: assume fee tier key follows vault_b (32 bytes) then u16 tick spacing
        let ft_off = *vb + 32;
        if data.len() >= ft_off + 32 + 2 {
            let k = Pubkey::new_from_array(data[ft_off..ft_off+32].try_into().unwrap());
            if k.to_bytes() != [0u8;32] { fee_tier_key = Some(k); }
            tick_spacing_detected = Some(u16::from_le_bytes([data[ft_off+32], data[ft_off+33]]));
        }
        // Heuristic tick index scan near end
        for off in (vb+32..data.len().saturating_sub(4)).step_by(4).take(32) {
            if off + 4 <= data.len() {
                let val = i32::from_le_bytes(data[off..off+4].try_into().unwrap());
                if val.abs() < 5_000_000 { tick_current_index = Some(val); break; }
            }
        }
        return Some(((mint_a, mint_b), vault_a, vault_b, WhirlpoolMeta { fee_bps: fee_bps_detected, tick_spacing: tick_spacing_detected, fee_tier_key, tick_current_index }));
            }
        }
        None
    }

    fn parse_token_amount(data: &[u8]) -> u64 {
        if data.len() < 72 { return 0; }
        let mut arr = [0u8;8];
        arr.copy_from_slice(&data[64..72]);
        u64::from_le_bytes(arr)
    }
}

#[derive(Debug, Clone)]
struct WhirlpoolMeta {
    fee_bps: u32,
    tick_spacing: Option<u16>,
    fee_tier_key: Option<Pubkey>,
    tick_current_index: Option<i32>,
}

fn derive_tick_array_pda(pool: &Pubkey, start_tick: i32) -> Pubkey {
    let seeds: &[&[u8]] = &[b"tick_array", pool.as_ref(), &start_tick.to_le_bytes()];
    Pubkey::find_program_address(seeds, &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).unwrap()).0
}

fn derive_oracle_pda(pool: &Pubkey) -> Pubkey {
    let seeds: &[&[u8]] = &[b"oracle", pool.as_ref()];
    Pubkey::find_program_address(seeds, &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).unwrap()).0
}

fn align_to_start(tick: i32, spacing: i32) -> i32 { tick - (tick.rem_euclid(spacing)) }
