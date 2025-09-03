
//! Raydium Connector – on-chain pool reader (Solana 2.x compatible)

use std::sync::Arc;
use std::str::FromStr;
use anyhow::{Result, anyhow};
use async_trait::async_trait;

use crate::solana::rpc::SolanaRpc;
use super::{Dex, Quote};
use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey; // SDK Pubkey (not Address wrapper)
use solana_client::rpc_config::{RpcProgramAccountsConfig, RpcAccountInfoConfig};
use solana_client::rpc_filter::{RpcFilterType, Memcmp, MemcmpEncodedBytes};
use solana_sdk::instruction::Instruction;

pub const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"; // verify
pub const OPENBOOK_V3: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
// Common Raydium AMM authority seed – for full implementation derive PDA if needed.

/// Parameters required for composing a real Raydium swap instruction (BaseIn variant).
/// Most can be sourced from the pool state; user_* and treasury authority come from caller.
#[derive(Clone, Debug)]
pub struct RaydiumSwapAccounts {
    pub user_authority: Pubkey,
    pub user_source: Pubkey,
    pub user_destination: Pubkey,
    pub amm_id: Pubkey,
    pub amm_authority: Pubkey,
    pub amm_open_orders: Pubkey,
    pub amm_target_orders: Pubkey,           // sometimes zeroed for v4
    pub pool_base_vault: Pubkey,
    pub pool_quote_vault: Pubkey,
    pub serum_program: Pubkey,
    pub serum_market: Pubkey,
    pub serum_bids: Pubkey,
    pub serum_asks: Pubkey,
    pub serum_event_queue: Pubkey,
    pub serum_base_vault: Pubkey,
    pub serum_quote_vault: Pubkey,
    pub serum_vault_signer: Pubkey,
    pub token_program: Pubkey,
    pub rent_sysvar: Pubkey,
}

pub struct Raydium {
    rpc: Arc<SolanaRpc>,
    pools: Arc<DashMap<Pubkey, SimplePool>>, // in-memory pool snapshot
}

/// Planned swap including the constructed instruction list plus metadata for future TX assembly.
#[derive(Debug, Clone)]
pub struct RaydiumSwapPlan {
    pub ixs: Vec<solana_sdk::instruction::Instruction>,
    pub amount_in: u64,
    pub min_out: u64,
    pub expected_out: u64,
    pub price_impact_bps: u32,
    pub fee_bps: u32,
    pub pool: Option<Pubkey>,
    pub compute_unit_limit: Option<u32>,
    pub compute_unit_price_micro_lamports: Option<u64>,
}

/// Lightweight public snapshot for backtesting / adapters without exposing internal struct.
#[derive(Debug, Clone)]
pub struct PoolSnapshot {
    pub address: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub reserve_base: u128,
    pub reserve_quote: u128,
    pub fee_bps: u32,
    pub open_orders: Option<Pubkey>,
    pub market_id: Option<Pubkey>,
    pub market_program_id: Option<Pubkey>,
    pub amm_authority: Option<Pubkey>,
    pub serum_vault_signer: Option<Pubkey>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SimplePool {
    base_mint: Pubkey,
    quote_mint: Pubkey,
    #[allow(dead_code)] base_vault: Pubkey,
    #[allow(dead_code)] quote_vault: Pubkey,
    #[allow(dead_code)] lp_reserve: u64,
    address: Pubkey,
    open_orders: Option<Pubkey>,
    market_id: Option<Pubkey>,
    market_program_id: Option<Pubkey>,
    amm_authority: Option<Pubkey>,
    serum_vault_signer: Option<Pubkey>,
    // cached reserves (token balances) in raw units
    reserve_base: u128,
    reserve_quote: u128,
    fee_bps: u32,
    last_update: std::time::SystemTime,
}

impl Raydium {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self { Self { rpc, pools: Arc::new(DashMap::new()) } }

    /// Export immutable pool snapshots (cheap clone of small fields) for backtest ingestion.
    pub fn snapshots(&self) -> Vec<PoolSnapshot> {
        self.pools.iter().map(|p| PoolSnapshot {
            address: p.address,
            base_mint: p.base_mint,
            quote_mint: p.quote_mint,
            reserve_base: p.reserve_base,
            reserve_quote: p.reserve_quote,
            fee_bps: p.fee_bps,
            open_orders: p.open_orders,
            market_id: p.market_id,
            market_program_id: p.market_program_id,
            amm_authority: p.amm_authority,
            serum_vault_signer: p.serum_vault_signer,
        }).collect()
    }

    fn parse_token_account_amount(data: &[u8]) -> Result<u64> {
        if data.len() < 72 { return Err(anyhow!("token account data too short")); }
        let amt_bytes: [u8;8] = data[64..72].try_into().map_err(|_| anyhow!("slice"))?;
        Ok(u64::from_le_bytes(amt_bytes))
    }

    fn pool_filters() -> Vec<RpcFilterType> {
        let mut filters = vec![RpcFilterType::DataSize(reader::LIQ_STATE_V4_SIZE as u64)];
        // active status == 6
        let status = 6u64.to_le_bytes().to_vec();
        filters.push(RpcFilterType::Memcmp(Memcmp::new(reader::offs::STATUS, MemcmpEncodedBytes::Bytes(status))));
        filters
    }

    fn program_id() -> Pubkey { Pubkey::from_str(RAYDIUM_AMM_V4).expect("raydium program id") }

    fn find_pool(&self, input: &Pubkey, output: &Pubkey) -> Option<(Pubkey, bool)> {
        // returns (pool_address, forward) where forward means input == base
        let mut best: Option<(Pubkey, bool, u128)> = None; // maximize liquidity (sum reserves)
        for p in self.pools.iter() {
            let forward = p.base_mint == *input && p.quote_mint == *output;
            let reverse = p.base_mint == *output && p.quote_mint == *input;
            if !forward && !reverse { continue; }
            let total_liq = p.reserve_base + p.reserve_quote;
            let better = best.as_ref().map(|(_,_,liq)| total_liq > *liq).unwrap_or(true);
            if better { best = Some((p.address, forward, total_liq)); }
        }
        best.map(|(addr,f,_ )| (addr,f))
    }

    /// Derive Raydium AMM authority PDA (shared across pools) using seed "amm authority".
    pub fn derive_amm_authority() -> (Pubkey, u8) {
        Pubkey::find_program_address(&[b"amm authority"], &Pubkey::from_str(RAYDIUM_AMM_V4).expect("prog"))
    }

    /// Derive Serum/OpenBook vault signer PDA for a given market id.
    pub fn derive_serum_vault_signer(market: &Pubkey, serum_program: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[market.as_ref()], serum_program)
    }

    /// Build a swap plan (base-in) with optional compute budget & priority fee.
    /// This uses the best single pool found in current snapshots.
    pub fn build_swap_plan(&self, input_mint: &str, output_mint: &str, amount_in: u64, slippage_bps: u32, compute_unit_limit: Option<u32>, compute_unit_price_micro_lamports: Option<u64>) -> Result<Option<RaydiumSwapPlan>> {
    // NOTE: compute budget program path differs across SDK versions; if unavailable, skip injecting.
        // 1) Get quote (sync helper using current snapshot; mirrors async path logic)
        let quote_opt = self.local_quote(input_mint, output_mint, amount_in);
        let quote = match quote_opt { Some(q) => q, None => return Ok(None) };
        // 2) Compute min_out
        let min_out = Self::apply_slippage_min_out(quote.amount_out, slippage_bps);
        // 3) Build swap ix (placeholder simplified variant)
        let swap_ixs = self.build_swap_ix(input_mint, output_mint, amount_in, min_out)?;
        let mut ixs = Vec::new();
    // (Temporarily disabled compute budget instructions due to missing module in current SDK compile context)
        ixs.extend(swap_ixs);
        // Try parse pool pubkey from route[0]
        let pool = quote.route.get(0).and_then(|s| Pubkey::from_str(s).ok());
        Ok(Some(RaydiumSwapPlan { ixs, amount_in, min_out, expected_out: quote.amount_out, price_impact_bps: quote.price_impact_bps, fee_bps: quote.fee_bps, pool, compute_unit_limit: compute_unit_limit, compute_unit_price_micro_lamports: compute_unit_price_micro_lamports }))
    }

    fn local_quote(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Option<Quote> {
        let in_pk = Pubkey::from_str(input_mint).ok()?;
        let out_pk = Pubkey::from_str(output_mint).ok()?;
        let mut best: Option<Quote> = None;
        for p in self.pools.iter() {
            let is_forward = p.base_mint == in_pk && p.quote_mint == out_pk;
            let is_reverse = p.base_mint == out_pk && p.quote_mint == in_pk;
            if !is_forward && !is_reverse { continue; }
            let (rin, rout) = if is_forward { (p.reserve_base, p.reserve_quote) } else { (p.reserve_quote, p.reserve_base) };
            if rin == 0 || rout == 0 { continue; }
            let fee_bps: u128 = p.fee_bps as u128;
            let amount_in_u = amount_in as u128;
            let amount_in_less_fee = amount_in_u * (10_000 - fee_bps) / 10_000;
            let numerator = amount_in_less_fee * rout;
            let denominator = rin + amount_in_less_fee;
            let out_amt = (numerator / denominator) as u64;
            if out_amt == 0 { continue; }
            let impact_bps = ((amount_in_less_fee * 10_000) / (rin + amount_in_less_fee)) as u32;
            let q = Quote { amount_out: out_amt, price_impact_bps: impact_bps, route: vec![p.address.to_string()], fee_bps: p.fee_bps, in_reserve: rin, out_reserve: rout };
            if best.as_ref().map(|b| b.amount_out < out_amt).unwrap_or(true) { best = Some(q); }
        }
        best
    }
}

#[async_trait]
impl Dex for Raydium {
    async fn refresh_pools(&self) -> Result<()> {
        use std::time::{SystemTime, Duration};
        tracing::trace!("raydium.refresh_pools() start");
        let acc_cfg = RpcAccountInfoConfig { encoding: None, data_slice: None, commitment: None, min_context_slot: None };
        let cfg = RpcProgramAccountsConfig { filters: Some(Self::pool_filters()), account_config: acc_cfg, with_context: None, sort_results: None };
        let program_id = Pubkey::from_str(RAYDIUM_AMM_V4)?;
        let accounts = self.rpc.rpc.get_program_accounts_with_config(&program_id, cfg).await?;
        // Collect decodable pool state + raw bytes for fee extraction
        let mut decoded: Vec<(reader::PoolV4, Vec<u8>)> = Vec::with_capacity(accounts.len());
        for (addr, acc) in &accounts {
            if acc.data.len() == reader::LIQ_STATE_V4_SIZE {
                if let Ok(p) = reader::PoolV4::decode(*addr, &acc.data) { decoded.push((p, acc.data.clone())); }
            }
        }
        // Batch vault fetch
        let mut vaults: Vec<Pubkey> = Vec::with_capacity(decoded.len() * 2);
        for (p, _) in &decoded { vaults.push(p.base_vault); vaults.push(p.quote_vault); }
        let mut vault_amounts = std::collections::HashMap::new();
        if !vaults.is_empty() {
            if let Ok(accts) = self.rpc.rpc.get_multiple_accounts(&vaults).await {
                for (i, acc_opt) in accts.into_iter().enumerate() {
                    if let Some(a) = acc_opt { if let Ok(val) = Self::parse_token_account_amount(&a.data) { vault_amounts.insert(vaults[i], val as u128); } }
                }
            }
        }
        // Insert/update
        for (p, raw) in decoded {
            let base_amt = vault_amounts.get(&p.base_vault).copied().unwrap_or(0);
            let quote_amt = vault_amounts.get(&p.quote_vault).copied().unwrap_or(0);
            let fee_num = raw.get(8..16).and_then(|s| s.try_into().ok()).map(u64::from_le_bytes).unwrap_or(25);
            let fee_den = raw.get(16..24).and_then(|s| s.try_into().ok()).map(u64::from_le_bytes).unwrap_or(10_000);
            let mut fee_bps = if fee_den > 0 && fee_num <= fee_den { ((fee_num * 10_000) / fee_den) as u32 } else { 25 };
            if fee_bps == 0 || fee_bps > 1000 { tracing::warn!(pool = %p.address, fee_bps, "abnormal fee, fallback 25"); fee_bps = 25; }
            let (amm_auth, _) = Self::derive_amm_authority();
            let serum_vault_signer = if p.market_program_id != Pubkey::default() {
                let (v, _) = Self::derive_serum_vault_signer(&p.market_id, &p.market_program_id);
                Some(v)
            } else { None };
            let obj = SimplePool { base_mint: p.base_mint, quote_mint: p.quote_mint, base_vault: p.base_vault, quote_vault: p.quote_vault, lp_reserve: p.lp_reserve, address: p.address, reserve_base: base_amt, reserve_quote: quote_amt, fee_bps, last_update: SystemTime::now(), open_orders: Some(p.open_orders), market_id: Some(p.market_id), market_program_id: Some(p.market_program_id), amm_authority: Some(amm_auth), serum_vault_signer };
            self.pools.insert(p.address, obj);
        }
        // Cleanup stale (>15m)
        let cutoff = SystemTime::now() - Duration::from_secs(15 * 60);
        let mut removed = 0u32;
        self.pools.retain(|_, v| { let keep = v.last_update >= cutoff; if !keep { removed += 1; } keep });
        tracing::info!(pools = self.pools.len(), removed, "raydium.refresh_pools done");
        Ok(())
    }

    async fn quote_exact_in(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Result<Option<Quote>> {
        use std::str::FromStr;
        let in_pk = Pubkey::from_str(input_mint).ok();
        let out_pk = Pubkey::from_str(output_mint).ok();
        if in_pk.is_none() || out_pk.is_none() { return Ok(None); }
        let in_pk = in_pk.unwrap();
        let out_pk = out_pk.unwrap();
        let mut best: Option<Quote> = None;
        for p in self.pools.iter() {
            let is_forward = p.base_mint == in_pk && p.quote_mint == out_pk;
            let is_reverse = p.base_mint == out_pk && p.quote_mint == in_pk;
            if !is_forward && !is_reverse { continue; }
            let (rin, rout) = if is_forward { (p.reserve_base, p.reserve_quote) } else { (p.reserve_quote, p.reserve_base) };
            if rin == 0 || rout == 0 { continue; }
            let fee_bps: u128 = p.fee_bps as u128;
            let amount_in_u = amount_in as u128;
            let amount_in_less_fee = amount_in_u * (10_000 - fee_bps) / 10_000;
            let numerator = amount_in_less_fee * rout;
            let denominator = rin + amount_in_less_fee;
            let out_amt = (numerator / denominator) as u64;
            if out_amt == 0 { continue; }
            // price impact approx: (amount_in / (rin + amount_in)) * 10_000
            let impact_bps = ((amount_in_less_fee * 10_000) / (rin + amount_in_less_fee)) as u32;
            let q = Quote {
                amount_out: out_amt,
                price_impact_bps: impact_bps,
                route: vec![p.address.to_string()],
                fee_bps: p.fee_bps,
                in_reserve: rin,
                out_reserve: rout,
            };
            if best.as_ref().map(|b| b.amount_out < out_amt).unwrap_or(true) { best = Some(q); }
        }
        Ok(best)
    }

    fn build_swap_ix(&self, input_mint: &str, output_mint: &str, amount_in: u64, min_out: u64) -> Result<Vec<Instruction>> {
        use std::str::FromStr;
        let in_pk = Pubkey::from_str(input_mint)?;
        let out_pk = Pubkey::from_str(output_mint)?;
        let (pool_addr, forward) = self.find_pool(&in_pk, &out_pk)
            .ok_or_else(|| anyhow!("no raydium pool for pair"))?;
        // Fetch pool snapshot for vaults & open_orders if present
        let pool_opt = self.pools.get(&pool_addr);
        if pool_opt.is_none() { return Err(anyhow!("pool snapshot missing")); }
        // Placeholder: open_orders & target_orders not stored; real implementation should extend SimplePool.
        // Provide a helper requiring explicit RaydiumSwapAccounts if caller wants real swap.
        let dir_flag = if forward { 0u8 } else { 1u8 };
        // Use pseudo layout identical to Raydium base-in swap: tag (u8)=9 (commonly), amount_in u64 LE, min_out u64 LE, direction flag u8.
        let mut data = Vec::with_capacity(1 + 8 + 8 + 1);
        data.push(9u8); // tentative Raydium SwapBaseIn tag
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());
        data.push(dir_flag);
        let pseudo_ix = Instruction {
            program_id: Self::program_id(),
            accounts: vec![solana_sdk::instruction::AccountMeta { pubkey: pool_addr, is_signer: false, is_writable: true }],
            data,
        };
        Ok(vec![pseudo_ix])
    }
}

impl Raydium {
    #[allow(dead_code)]
    pub fn apply_slippage_min_out(quote_amount_out: u64, slippage_bps: u32) -> u64 {
        if slippage_bps == 0 { return quote_amount_out; }
        let keep = 10_000u64.saturating_sub(slippage_bps as u64);
        (quote_amount_out as u128 * keep as u128 / 10_000u128) as u64
    }

    #[allow(dead_code)]
    pub fn compute_min_out_from_quote(q: &Quote, slippage_bps: u32) -> u64 {
        Self::apply_slippage_min_out(q.amount_out, slippage_bps)
    }

    /// Build a full Raydium swap (base in) using explicit account list. This bypasses internal pool inference.
    #[allow(dead_code)]
    pub fn build_full_swap(accounts: RaydiumSwapAccounts, amount_in: u64, min_out: u64, direction_forward: bool) -> Instruction {
        // Raydium swap base-in instruction tag (commonly 9). Layout: tag u8, amount_in u64, min_out u64.
        let mut data = Vec::with_capacity(1 + 8 + 8 + 1);
        data.push(9u8);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());
        data.push(if direction_forward { 0 } else { 1 });
        use solana_sdk::instruction::AccountMeta as AM;
        Instruction {
            program_id: accounts.amm_id, // Raydium program id expected
            accounts: vec![
                AM { pubkey: accounts.amm_id, is_signer: false, is_writable: true },
                AM { pubkey: accounts.amm_authority, is_signer: false, is_writable: false },
                AM { pubkey: accounts.amm_open_orders, is_signer: false, is_writable: true },
                AM { pubkey: accounts.amm_target_orders, is_signer: false, is_writable: true },
                AM { pubkey: accounts.pool_base_vault, is_signer: false, is_writable: true },
                AM { pubkey: accounts.pool_quote_vault, is_signer: false, is_writable: true },
                AM { pubkey: accounts.serum_market, is_signer: false, is_writable: true },
                AM { pubkey: accounts.serum_bids, is_signer: false, is_writable: true },
                AM { pubkey: accounts.serum_asks, is_signer: false, is_writable: true },
                AM { pubkey: accounts.serum_event_queue, is_signer: false, is_writable: true },
                AM { pubkey: accounts.serum_base_vault, is_signer: false, is_writable: true },
                AM { pubkey: accounts.serum_quote_vault, is_signer: false, is_writable: true },
                AM { pubkey: accounts.serum_vault_signer, is_signer: false, is_writable: false },
                AM { pubkey: accounts.user_source, is_signer: false, is_writable: true },
                AM { pubkey: accounts.user_destination, is_signer: false, is_writable: true },
                AM { pubkey: accounts.user_authority, is_signer: true, is_writable: false },
                AM { pubkey: accounts.token_program, is_signer: false, is_writable: false },
                AM { pubkey: accounts.rent_sysvar, is_signer: false, is_writable: false },
                AM { pubkey: accounts.serum_program, is_signer: false, is_writable: false },
            ],
            data,
        }
    }
}

/// --- On-chain pool reader subset (blocking client for CLI) ---
pub mod reader {
    use anyhow::{anyhow, Result};
    use solana_client::rpc_client::RpcClient;
    use solana_client::rpc_config::{RpcProgramAccountsConfig, RpcAccountInfoConfig};
    use solana_client::rpc_filter::{RpcFilterType, Memcmp, MemcmpEncodedBytes};
    use solana_sdk::{account::Account, pubkey::Pubkey};

    pub const LIQ_STATE_V4_SIZE: usize = 752;
    pub mod offs {
        pub const BASE_VAULT: usize = 336;
        pub const QUOTE_VAULT: usize = 368;
        pub const BASE_MINT: usize = 400;
        pub const QUOTE_MINT: usize = 432;
        pub const LP_MINT: usize = 464;
        pub const OPEN_ORDERS: usize = 496;
        pub const MARKET_ID: usize = 528;
        pub const MARKET_PROGRAM_ID: usize = 560;
        pub const LP_RESERVE: usize = 720; // u64 LE
        pub const STATUS: usize = 0;       // u64 LE
    }

    #[derive(Debug, Clone)]
    pub struct PoolV4 {
        pub address: Pubkey,
        pub base_mint: Pubkey,
        pub quote_mint: Pubkey,
        pub lp_mint: Pubkey,
        pub base_vault: Pubkey,
        pub quote_vault: Pubkey,
        pub open_orders: Pubkey,
        pub market_id: Pubkey,
        pub market_program_id: Pubkey,
        pub lp_reserve: u64,
        pub raw_account: Option<Account>,
    }

    impl PoolV4 {
        pub fn decode(addr: Pubkey, data: &[u8]) -> Result<Self> {
            if data.len() != LIQ_STATE_V4_SIZE {
                return Err(anyhow!("unexpected data size: {}", data.len()));
            }
            let read_pk = |o: usize| -> Result<Pubkey> {
                let bytes: &[u8; 32] = data[o..o+32].try_into().map_err(|_| anyhow!("slice -> [u8;32] failed at {o}"))?;
                Ok(Pubkey::new_from_array(*bytes))
            };
            let read_u64 = |o: usize| -> Result<u64> {
                let arr: [u8; 8] = data[o..o+8].try_into().map_err(|_| anyhow!("slice -> [u8;8] failed at {o}"))?;
                Ok(u64::from_le_bytes(arr))
            };
            Ok(Self {
                address: addr,
                base_mint: read_pk(offs::BASE_MINT)?,
                quote_mint: read_pk(offs::QUOTE_MINT)?,
                lp_mint: read_pk(offs::LP_MINT)?,
                base_vault: read_pk(offs::BASE_VAULT)?,
                quote_vault: read_pk(offs::QUOTE_VAULT)?,
                open_orders: read_pk(offs::OPEN_ORDERS)?,
                market_id: read_pk(offs::MARKET_ID)?,
                market_program_id: read_pk(offs::MARKET_PROGRAM_ID)?,
                lp_reserve: read_u64(offs::LP_RESERVE)?,
                raw_account: None,
            })
        }
    }

    pub fn fetch_pools(
        rpc: &RpcClient,
        base: Option<Pubkey>,
        quote: Option<Pubkey>,
        active_only: bool,
        with_accounts: bool,
        program_id: Pubkey,
    ) -> Result<Vec<PoolV4>> {
        let mut filters: Vec<RpcFilterType> = vec![RpcFilterType::DataSize(LIQ_STATE_V4_SIZE as u64)];
        if let Some(b) = base {
            let bytes = b.to_bytes().to_vec();
            let memcmp = Memcmp::new(offs::BASE_MINT, MemcmpEncodedBytes::Bytes(bytes));
            filters.push(RpcFilterType::Memcmp(memcmp));
        }
        if let Some(q) = quote {
            let bytes = q.to_bytes().to_vec();
            let memcmp = Memcmp::new(offs::QUOTE_MINT, MemcmpEncodedBytes::Bytes(bytes));
            filters.push(RpcFilterType::Memcmp(memcmp));
        }
        if active_only {
            let status = 6u64.to_le_bytes().to_vec();
            let memcmp = Memcmp::new(offs::STATUS, MemcmpEncodedBytes::Bytes(status));
            filters.push(RpcFilterType::Memcmp(memcmp));
        }

        let acc_cfg = RpcAccountInfoConfig {
            encoding: None,
            data_slice: None,
            commitment: None,        // use node default
            min_context_slot: None,
        };

        let cfg = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: acc_cfg,
            with_context: None,
            sort_results: None,      // new in Agave 2.x
        };

        let items = rpc.get_program_accounts_with_config(&program_id, cfg)?;
        let mut out = Vec::with_capacity(items.len());
        for (addr, mut acc) in items {
            let mut d = PoolV4::decode(addr, &acc.data)?;
            if with_accounts {
                d.raw_account = Some(acc);
            } else {
                acc.data.clear();
            }
            out.push(d);
        }
        Ok(out)
    }
}
