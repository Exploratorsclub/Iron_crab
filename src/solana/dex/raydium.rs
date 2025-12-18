//! Raydium Connector – on-chain pool reader (Solana 2.x compatible)

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use std::str::FromStr;
use std::sync::Arc;
use tracing::debug;

use super::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;
use dashmap::DashMap;
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::RpcFilterType;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey; // SDK Pubkey (not Address wrapper)

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
    pub amm_target_orders: Pubkey, // sometimes zeroed for v4
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

/// External Serum (OpenBook) market accounts required for composing a Raydium swap.
/// These are fetched / derived outside (we do not yet cache full Serum market state in the Raydium adapter).
#[derive(Clone, Debug)]
pub struct SerumMarketAccounts {
    pub bids: Pubkey,
    pub asks: Pubkey,
    pub event_queue: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
}

pub struct Raydium {
    rpc: Arc<SolanaRpc>,
    pools: Arc<DashMap<Pubkey, SimplePool>>, // in-memory pool snapshot
    mint_index: Arc<DashMap<Pubkey, Vec<Pubkey>>>, // mint -> pools containing it
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
    pub target_orders: Option<Pubkey>,
    pub base_vault: Option<Pubkey>,
    pub quote_vault: Option<Pubkey>,
    pub serum_bids: Option<Pubkey>,
    pub serum_asks: Option<Pubkey>,
    pub serum_event_queue: Option<Pubkey>,
    pub serum_base_vault: Option<Pubkey>,
    pub serum_quote_vault: Option<Pubkey>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SimplePool {
    base_mint: Pubkey,
    quote_mint: Pubkey,
    #[allow(dead_code)]
    base_vault: Pubkey,
    #[allow(dead_code)]
    quote_vault: Pubkey,
    #[allow(dead_code)]
    lp_reserve: u64,
    address: Pubkey,
    open_orders: Option<Pubkey>,
    market_id: Option<Pubkey>,
    market_program_id: Option<Pubkey>,
    amm_authority: Option<Pubkey>,
    serum_vault_signer: Option<Pubkey>,
    target_orders: Option<Pubkey>,
    // cached reserves (token balances) in raw units
    reserve_base: u128,
    reserve_quote: u128,
    fee_bps: u32,
    last_update: std::time::SystemTime,
    serum_bids: Option<Pubkey>,
    serum_asks: Option<Pubkey>,
    serum_event_queue: Option<Pubkey>,
    serum_base_vault: Option<Pubkey>,
    serum_quote_vault: Option<Pubkey>,
}

impl Raydium {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self {
            rpc,
            pools: Arc::new(DashMap::new()),
            mint_index: Arc::new(DashMap::new()),
        }
    }

    /// Get total liquidity in SOL for a given mint across all tracked pools.
    pub fn get_liquidity_sol_for_mint(&self, mint: &Pubkey) -> f64 {
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        let mut total_sol = 0.0;

        if let Some(pools) = self.mint_index.get(mint) {
            for pool_pubkey in pools.iter() {
                if let Some(pool) = self.pools.get(pool_pubkey) {
                    if pool.base_mint == sol_mint {
                        total_sol += pool.reserve_base as f64 / 1e9;
                    } else if pool.quote_mint == sol_mint {
                        total_sol += pool.reserve_quote as f64 / 1e9;
                    }
                }
            }
        }
        total_sol
    }

    /// Load a pool from Geyser discovery event into the cache.
    /// This allows the bot to trade on newly detected pools immediately.
    pub async fn load_pool_from_geyser(&self, pool_address: &Pubkey) -> Result<()> {
        // Fetch the full pool account data from RPC with retry for brand new pools
        // Extended retry window: 20 attempts × 500ms = 10 seconds total
        const MAX_RETRIES: usize = 20;
        const RETRY_DELAY_MS: u64 = 500;

        let account = {
            let mut last_error = None;
            let mut account_opt = None;

            for attempt in 0..MAX_RETRIES {
                match self.rpc.get_account_retry(pool_address).await {
                    Ok(acc) => {
                        if attempt > 0 {
                            debug!(pool=%pool_address, attempt, "raydium: pool account fetch succeeded after retry");
                        }
                        account_opt = Some(acc);
                        break;
                    }
                    Err(e) => {
                        last_error = Some(e);
                        if attempt < MAX_RETRIES - 1 {
                            debug!(pool=%pool_address, attempt, "raydium: pool account not found, retrying...");
                            tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS))
                                .await;
                        }
                    }
                }
            }

            match account_opt {
                Some(acc) => acc,
                None => {
                    return Err(anyhow!(
                        "failed to fetch raydium pool account after {} retries: {:?}",
                        MAX_RETRIES,
                        last_error
                    ))
                }
            }
        };

        // Parse using the reader module
        let pool_state = reader::PoolV4::decode(*pool_address, &account.data)?;

        // Validate the pool
        Self::validate_pool_state(&pool_state)?;

        // Derive PDAs
        let (amm_auth, _) = Self::derive_amm_authority();
        let serum_vault_signer = if pool_state.market_program_id.to_bytes() != [0u8; 32] {
            let (v, _) = Self::derive_serum_vault_signer(
                &pool_state.market_id,
                &pool_state.market_program_id,
            );
            Some(v)
        } else {
            None
        };

        // Create SimplePool entry
        let pool = SimplePool {
            base_mint: pool_state.base_mint,
            quote_mint: pool_state.quote_mint,
            base_vault: pool_state.base_vault,
            quote_vault: pool_state.quote_vault,
            lp_reserve: pool_state.lp_reserve,
            address: *pool_address,
            open_orders: Some(pool_state.open_orders),
            market_id: Some(pool_state.market_id),
            market_program_id: Some(pool_state.market_program_id),
            amm_authority: Some(amm_auth),
            serum_vault_signer,
            target_orders: pool_state.target_orders,
            reserve_base: 0, // Will be updated by vault fetch
            reserve_quote: 0,
            fee_bps: 30, // Default Raydium fee
            last_update: std::time::SystemTime::now(),
            serum_bids: None,
            serum_asks: None,
            serum_event_queue: None,
            serum_base_vault: None,
            serum_quote_vault: None,
        };

        // Insert into pools cache
        self.pools.insert(*pool_address, pool.clone());

        // Update mint index
        self.mint_index
            .entry(pool.base_mint)
            .or_default()
            .push(*pool_address);
        self.mint_index
            .entry(pool.quote_mint)
            .or_default()
            .push(*pool_address);

        tracing::info!(
            pool=%pool_address,
            base=%pool.base_mint,
            quote=%pool.quote_mint,
            "loaded raydium pool from geyser into cache"
        );

        Ok(())
    }

    /// Check if a pool is already cached
    pub fn pool_exists(&self, pool_address: &Pubkey) -> bool {
        self.pools.contains_key(pool_address)
    }

    /// Check if a mint is known in the cache
    pub fn is_mint_known(&self, mint: &Pubkey) -> bool {
        self.mint_index.contains_key(mint)
    }

    /// Export immutable pool snapshots (cheap clone of small fields) for backtest ingestion.
    pub fn snapshots(&self) -> Vec<PoolSnapshot> {
        self.pools
            .iter()
            .map(|p| PoolSnapshot {
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
                target_orders: p.target_orders,
                base_vault: Some(p.base_vault),
                quote_vault: Some(p.quote_vault),
                serum_bids: p.serum_bids,
                serum_asks: p.serum_asks,
                serum_event_queue: p.serum_event_queue,
                serum_base_vault: p.serum_base_vault,
                serum_quote_vault: p.serum_quote_vault,
            })
            .collect()
    }

    /// Pools that reference this mint.
    pub fn pools_for_mint(&self, mint: &Pubkey) -> Vec<Pubkey> {
        self.mint_index
            .get(mint)
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    #[allow(dead_code)]
    fn parse_token_account_amount(data: &[u8]) -> Result<u64> {
        if data.len() < 72 {
            return Err(anyhow!("token account data too short"));
        }
        let amt_bytes: [u8; 8] = data[64..72].try_into().map_err(|_| anyhow!("slice"))?;
        Ok(u64::from_le_bytes(amt_bytes))
    }

    fn pool_filters() -> Vec<RpcFilterType> {
        // Only filter by data size - status filtering excluded as it's unreliable
        // Raydium AMM v4 pools have fixed size 752 bytes
        // We validate pool state (including status) after decoding
        vec![RpcFilterType::DataSize(reader::LIQ_STATE_V4_SIZE as u64)]
    }

    /// Hard validation (errors) + soft warnings for pool structural invariants.
    fn validate_pool_state(p: &reader::PoolV4) -> Result<()> {
        ensure!(p.base_mint != p.quote_mint, "pool base_mint == quote_mint");
        ensure!(
            p.base_vault != p.quote_vault,
            "pool base_vault == quote_vault"
        );
        ensure!(
            p.base_vault.to_bytes() != [0u8; 32],
            "pool base_vault default pubkey"
        );
        ensure!(
            p.quote_vault.to_bytes() != [0u8; 32],
            "pool quote_vault default pubkey"
        );
        ensure!(
            p.open_orders.to_bytes() != [0u8; 32],
            "open_orders default pubkey"
        );
        // Market linkage: only enforce market_id if market_program_id set
        if p.market_program_id.to_bytes() != [0u8; 32] {
            ensure!(
                p.market_id.to_bytes() != [0u8; 32],
                "market_program set but market_id default"
            );
        }
        // Soft warnings
        if p.target_orders.is_none() {
            tracing::debug!(pool = %p.address, "raydium pool missing target_orders (soft)");
        }
        Ok(())
    }

    fn program_id() -> Pubkey {
        Pubkey::from_str(RAYDIUM_AMM_V4).expect("raydium program id")
    }

    fn find_pool(&self, input: &Pubkey, output: &Pubkey) -> Option<(Pubkey, bool)> {
        // returns (pool_address, forward) where forward means input == base
        let mut best: Option<(Pubkey, bool, u128)> = None; // maximize liquidity (sum reserves)
        for p in self.pools.iter() {
            let forward = p.base_mint == *input && p.quote_mint == *output;
            let reverse = p.base_mint == *output && p.quote_mint == *input;
            if !forward && !reverse {
                continue;
            }
            let total_liq = p.reserve_base + p.reserve_quote;
            let better = best
                .as_ref()
                .map(|(_, _, liq)| total_liq > *liq)
                .unwrap_or(true);
            if better {
                best = Some((p.address, forward, total_liq));
            }
        }
        best.map(|(addr, f, _)| (addr, f))
    }

    /// Derive Raydium AMM authority PDA (shared across pools) using seed "amm authority".
    pub fn derive_amm_authority() -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[b"amm authority"],
            &Pubkey::from_str(RAYDIUM_AMM_V4).expect("prog"),
        )
    }

    /// Derive Serum/OpenBook vault signer PDA for a given market id.
    pub fn derive_serum_vault_signer(market: &Pubkey, serum_program: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[market.as_ref()], serum_program)
    }

    /// Build a swap plan (base-in) with optional compute budget & priority fee.
    /// This uses the best single pool found in current snapshots.
    pub fn build_swap_plan(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        slippage_bps: u32,
        compute_unit_limit: Option<u32>,
        compute_unit_price_micro_lamports: Option<u64>,
    ) -> Result<Option<RaydiumSwapPlan>> {
        // Inject ComputeBudget + Priority Fee instructions (Solana 3.x SDK) when requested.
        // 1) Get quote (sync helper using current snapshot; mirrors async path logic)
        let quote_opt = self.local_quote(input_mint, output_mint, amount_in);
        let quote = match quote_opt {
            Some(q) => q,
            None => return Ok(None),
        };
        // 2) Compute min_out
        let min_out = Self::apply_slippage_min_out(quote.amount_out, slippage_bps);
        // Runtime validation (hard) + debug assertions
        let recomputed = Self::compute_min_out_from_quote(&quote, slippage_bps);
        debug_assert_eq!(min_out, recomputed, "min_out derivation mismatch (debug)");
        debug_assert!(
            min_out <= quote.amount_out,
            "min_out cannot exceed quoted amount (debug)"
        );
        debug_assert!(min_out > 0, "min_out must be > 0 (debug)");
        ensure!(
            min_out == recomputed,
            "Raydium swap plan min_out mismatch ({} != {})",
            min_out,
            recomputed
        );
        ensure!(
            min_out <= quote.amount_out,
            "Raydium swap plan min_out exceeds quote (min_out {} > quote {})",
            min_out,
            quote.amount_out
        );
        ensure!(min_out > 0, "Raydium swap plan min_out is zero");
        // 3) Build swap ix (placeholder simplified variant)
        let swap_ixs = self.build_swap_ix(input_mint, output_mint, amount_in, min_out)?;
        let mut ixs = Vec::new();
        if compute_unit_limit.is_some() || compute_unit_price_micro_lamports.is_some() {
            use crate::solana::compute_budget_helper as cbh;
            if let Some(limit) = compute_unit_limit {
                ixs.push(cbh::set_compute_unit_limit(limit));
            }
            if let Some(price) = compute_unit_price_micro_lamports {
                ixs.push(cbh::set_compute_unit_price(price));
            }
        }
        ixs.extend(swap_ixs);
        // Try parse pool pubkey from route[0]
        let pool = quote.route.first().and_then(|s| Pubkey::from_str(s).ok());
        Ok(Some(RaydiumSwapPlan {
            ixs,
            amount_in,
            min_out,
            expected_out: quote.amount_out,
            price_impact_bps: quote.price_impact_bps,
            fee_bps: quote.fee_bps,
            pool,
            compute_unit_limit,
            compute_unit_price_micro_lamports,
        }))
    }

    fn local_quote(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Option<Quote> {
        let in_pk = Pubkey::from_str(input_mint).ok()?;
        let out_pk = Pubkey::from_str(output_mint).ok()?;
        let mut best: Option<Quote> = None;
        for p in self.pools.iter() {
            let is_forward = p.base_mint == in_pk && p.quote_mint == out_pk;
            let is_reverse = p.base_mint == out_pk && p.quote_mint == in_pk;
            if !is_forward && !is_reverse {
                continue;
            }
            let (rin, rout) = if is_forward {
                (p.reserve_base, p.reserve_quote)
            } else {
                (p.reserve_quote, p.reserve_base)
            };
            if rin == 0 || rout == 0 {
                continue;
            }
            let fee_bps: u128 = p.fee_bps as u128;
            let amount_in_u = amount_in as u128;
            let amount_in_less_fee = amount_in_u * (10_000 - fee_bps) / 10_000;
            let numerator = amount_in_less_fee * rout;
            let denominator = rin + amount_in_less_fee;
            let out_amt = (numerator / denominator) as u64;
            if out_amt == 0 {
                continue;
            }
            let impact_bps = ((amount_in_less_fee * 10_000) / (rin + amount_in_less_fee)) as u32;
            let q = Quote {
                amount_out: out_amt,
                price_impact_bps: impact_bps,
                route: vec![p.address.to_string()],
                fee_bps: p.fee_bps,
                in_reserve: rin,
                out_reserve: rout,
                input_mint: (if is_forward { in_pk } else { out_pk }).to_string(),
                output_mint: (if is_forward { out_pk } else { in_pk }).to_string(),
                tick_spacing: None,
            };
            if best
                .as_ref()
                .map(|b| b.amount_out < out_amt)
                .unwrap_or(true)
            {
                best = Some(q);
            }
        }
        best
    }

    /// Build a swap plan with heuristic compute budget estimation when caller does not want to specify limits manually.
    pub async fn build_swap_plan_auto(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        slippage_bps: u32,
    ) -> Result<Option<RaydiumSwapPlan>> {
        use std::str::FromStr;
        let in_pk = Pubkey::from_str(input_mint)?;
        let out_pk = Pubkey::from_str(output_mint)?;

        // Find the pool
        let (pool_addr, _forward) = self
            .find_pool(&in_pk, &out_pk)
            .ok_or_else(|| anyhow!("no raydium pool for pair"))?;

        // Check if pool has Serum accounts populated, if not fetch them
        let (needs_serum, needs_reserves) = {
            let pool = self
                .pools
                .get(&pool_addr)
                .ok_or_else(|| anyhow!("pool snapshot missing"))?;
            (
                pool.serum_bids.is_none() || pool.serum_asks.is_none(),
                pool.reserve_base == 0 || pool.reserve_quote == 0,
            )
        };

        if needs_serum {
            if let Err(e) = self.fetch_and_populate_serum_accounts(&pool_addr).await {
                tracing::warn!(
                    pool=%pool_addr,
                    error=%e,
                    "failed to fetch serum accounts, swap will likely fail"
                );
                // Don't return error, let the swap attempt continue (will fail later with better error)
            }
        }

        if needs_reserves {
            if let Err(e) = self.fetch_and_update_reserves(&pool_addr).await {
                tracing::warn!(
                    pool=%pool_addr,
                    error=%e,
                    "failed to fetch pool reserves, swap calculation might fail"
                );
            }
        }

        let base =
            self.build_swap_plan(input_mint, output_mint, amount_in, slippage_bps, None, None)?;
        let Some(_plan) = base else {
            return Ok(None);
        };
        let est = crate::solana::compute_budget_estimator::estimate_single_swap(amount_in);
        // Only rebuild if estimate differs from defaults (currently defaults: none)
        let rebuilt = self.build_swap_plan(
            input_mint,
            output_mint,
            amount_in,
            slippage_bps,
            Some(est.compute_unit_limit),
            Some(est.compute_unit_price_micro_lamports),
        )?;
        Ok(rebuilt)
    }

    /// Replay: refresh pools using a ReplayRpc store (synchronous). Populates in-memory snapshots like live refresh.
    pub fn refresh_pools_replay(
        &self,
        replay: &crate::backtest::replay_rpc::ReplayRpc,
    ) -> anyhow::Result<()> {
        use reader::fetch_pools_replay;
        let decoded = fetch_pools_replay(replay, None, None, true, true, Self::program_id())?;
        // Build a quick map of vault balances if token account bytes are present in replay (optional). For replay MVP, keep reserves as zeros; rely on account CfmPriceUpdate events to set reserves later.
        for p in decoded {
            // Apply validate checks similar to live path, but softened for replay.
            if let Err(e) = Self::validate_pool_state(&p) {
                tracing::debug!(pool = %p.address, error = %e, "skip invalid raydium pool in replay");
                continue;
            }
            // In replay, reserves may be filled by CfmPriceUpdate; initialize with zeros.
            let (amm_auth, _) = Self::derive_amm_authority();
            let serum_vault_signer = if p.market_program_id != solana_sdk::pubkey::Pubkey::default()
            {
                let (v, _) = Self::derive_serum_vault_signer(&p.market_id, &p.market_program_id);
                Some(v)
            } else {
                None
            };
            let obj = SimplePool {
                base_mint: p.base_mint,
                quote_mint: p.quote_mint,
                base_vault: p.base_vault,
                quote_vault: p.quote_vault,
                lp_reserve: p.lp_reserve,
                address: p.address,
                reserve_base: 0,
                reserve_quote: 0,
                fee_bps: 30, // conservative default; will be overridden by Cfm updates if present
                last_update: std::time::SystemTime::now(),
                open_orders: Some(p.open_orders),
                market_id: Some(p.market_id),
                market_program_id: Some(p.market_program_id),
                amm_authority: Some(amm_auth),
                serum_vault_signer,
                target_orders: p.target_orders,
                serum_bids: None,
                serum_asks: None,
                serum_event_queue: None,
                serum_base_vault: None,
                serum_quote_vault: None,
            };
            self.pools.insert(p.address, obj);
        }
        // Rebuild mint index
        self.mint_index.clear();
        for p in self.pools.iter() {
            self.mint_index
                .entry(p.base_mint)
                .or_insert_with(|| Vec::with_capacity(2))
                .push(p.address);
            self.mint_index
                .entry(p.quote_mint)
                .or_insert_with(|| Vec::with_capacity(2))
                .push(p.address);
        }
        Ok(())
    }
}

#[async_trait]
impl Dex for Raydium {
    async fn refresh_pools(&self) -> Result<()> {
        use crate::metrics::{RAYDIUM_POOLS_LOADED, RAYDIUM_POOLS_SKIPPED_INVALID};
        use std::time::{Duration, SystemTime};
        tracing::trace!("raydium.refresh_pools() start");
        let acc_cfg = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            data_slice: None,
            commitment: None,
            min_context_slot: None,
        };
        let cfg = RpcProgramAccountsConfig {
            filters: Some(Self::pool_filters()),
            account_config: acc_cfg,
            with_context: None,
            sort_results: None,
        };
        let program_id = Pubkey::from_str(RAYDIUM_AMM_V4)?;
        let accounts = self
            .rpc
            .get_program_accounts_with_config_retry(&program_id, cfg)
            .await?;
        tracing::info!(
            program = %program_id,
            fetched = accounts.len(),
            "raydium.refresh_pools fetched program accounts"
        );
        // Collect decodable pool state + raw bytes for fee extraction
        let mut decoded: Vec<(reader::PoolV4, Vec<u8>)> = Vec::with_capacity(accounts.len());
        let mut wrong_size = 0u32;
        for (addr, acc) in &accounts {
            if acc.data.len() == reader::LIQ_STATE_V4_SIZE {
                if let Ok(p) = reader::PoolV4::decode(*addr, &acc.data) {
                    if let Err(e) = Self::validate_pool_state(&p) {
                        tracing::warn!(pool = %p.address, error = %e, "skip invalid raydium pool");
                        RAYDIUM_POOLS_SKIPPED_INVALID
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        decoded.push((p, acc.data.clone()));
                    }
                }
            } else {
                wrong_size += 1;
            }
        }
        tracing::info!(
            decoded = decoded.len(),
            wrong_size,
            "raydium.refresh_pools decoded candidate pools"
        );

        // Don't fetch vault balances upfront - use lazy-loading like Orca
        // Reserves will be fetched on-demand when pools are evaluated for quotes
        tracing::info!(
            decoded = decoded.len(),
            "raydium.refresh_pools decoded pools (reserves lazy-loaded)"
        );

        // Insert/update pools with zero reserves (will be fetched on-demand)
        let mut loaded = 0u32;
        for (p, _) in decoded {
            // Raydium AMM v4 uses a hardcoded fee of 25 basis points (0.25%)
            // The fee structure is fixed in the program, not stored per-pool
            let fee_bps = 25u32;
            let (amm_auth, _) = Self::derive_amm_authority();
            // Serum market is optional in modern Raydium v4 - try to fetch if available, but don't skip if missing
            let (
                serum_bids,
                serum_asks,
                serum_event_queue,
                serum_base_vault,
                serum_quote_vault,
                serum_vault_signer,
            ) = if p.market_id != Pubkey::default() && p.market_program_id != Pubkey::default() {
                // Try to fetch and parse Serum market accounts
                match self.rpc.rpc.get_account(&p.market_id).await {
                    Ok(acct) => match Self::parse_serum_market_accounts(&acct.data) {
                        Some((b, a, e, bv, qv)) => {
                            let (v, _) =
                                Self::derive_serum_vault_signer(&p.market_id, &p.market_program_id);
                            (b, a, e, bv, qv, Some(v))
                        }
                        None => (None, None, None, None, None, None),
                    },
                    Err(_) => (None, None, None, None, None, None),
                }
            } else {
                // No Serum market linkage - that's fine
                (None, None, None, None, None, None)
            };
            let obj = SimplePool {
                base_mint: p.base_mint,
                quote_mint: p.quote_mint,
                base_vault: p.base_vault,
                quote_vault: p.quote_vault,
                lp_reserve: p.lp_reserve,
                address: p.address,
                reserve_base: 0,  // Lazy-loaded on-demand
                reserve_quote: 0, // Lazy-loaded on-demand
                fee_bps,
                last_update: SystemTime::now(),
                open_orders: Some(p.open_orders),
                market_id: Some(p.market_id),
                market_program_id: Some(p.market_program_id),
                amm_authority: Some(amm_auth),
                serum_vault_signer,
                target_orders: p.target_orders,
                serum_bids,
                serum_asks,
                serum_event_queue,
                serum_base_vault,
                serum_quote_vault,
            };
            self.pools.insert(p.address, obj);
            RAYDIUM_POOLS_LOADED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            loaded += 1;
        }
        // Cleanup stale (>15m)
        let cutoff = SystemTime::now() - Duration::from_secs(15 * 60);
        let mut removed = 0u32;
        self.pools.retain(|_, v| {
            let keep = v.last_update >= cutoff;
            if !keep {
                removed += 1;
            }
            keep
        });
        // Rebuild mint index
        self.mint_index.clear();
        for p in self.pools.iter() {
            self.mint_index
                .entry(p.base_mint)
                .or_insert_with(|| Vec::with_capacity(2))
                .push(p.address);
            self.mint_index
                .entry(p.quote_mint)
                .or_insert_with(|| Vec::with_capacity(2))
                .push(p.address);
        }

        // Update pools_total metric
        crate::metrics::RAYDIUM_POOLS_TOTAL.store(
            self.pools.len() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        tracing::info!(
            pools = self.pools.len(),
            removed,
            loaded,
            "raydium.refresh_pools done"
        );
        Ok(())
    }

    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>> {
        use std::str::FromStr;
        let in_pk = Pubkey::from_str(input_mint).ok();
        let out_pk = Pubkey::from_str(output_mint).ok();
        if in_pk.is_none() || out_pk.is_none() {
            return Ok(None);
        }
        let in_pk = in_pk.unwrap();
        let out_pk = out_pk.unwrap();
        let mut best: Option<Quote> = None;
        for p in self.pools.iter() {
            let is_forward = p.base_mint == in_pk && p.quote_mint == out_pk;
            let is_reverse = p.base_mint == out_pk && p.quote_mint == in_pk;
            if !is_forward && !is_reverse {
                continue;
            }
            let (rin, rout) = if is_forward {
                (p.reserve_base, p.reserve_quote)
            } else {
                (p.reserve_quote, p.reserve_base)
            };
            if rin == 0 || rout == 0 {
                continue;
            }
            let fee_bps: u128 = p.fee_bps as u128;
            let amount_in_u = amount_in as u128;
            let amount_in_less_fee = amount_in_u * (10_000 - fee_bps) / 10_000;
            let numerator = amount_in_less_fee * rout;
            let denominator = rin + amount_in_less_fee;
            let out_amt = (numerator / denominator) as u64;
            if out_amt == 0 {
                continue;
            }
            // price impact approx: (amount_in / (rin + amount_in)) * 10_000
            let impact_bps = ((amount_in_less_fee * 10_000) / (rin + amount_in_less_fee)) as u32;
            let q = Quote {
                amount_out: out_amt,
                price_impact_bps: impact_bps,
                route: vec![p.address.to_string()],
                fee_bps: p.fee_bps,
                in_reserve: rin,
                out_reserve: rout,
                input_mint: (if is_forward { in_pk } else { out_pk }).to_string(),
                output_mint: (if is_forward { out_pk } else { in_pk }).to_string(),
                tick_spacing: None,
            };
            if best
                .as_ref()
                .map(|b| b.amount_out < out_amt)
                .unwrap_or(true)
            {
                best = Some(q);
            }
        }
        Ok(best)
    }

    fn build_swap_ix(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>> {
        use std::str::FromStr;
        let in_pk = Pubkey::from_str(input_mint)?;
        let out_pk = Pubkey::from_str(output_mint)?;
        let (pool_addr, _forward) = self
            .find_pool(&in_pk, &out_pk)
            .ok_or_else(|| anyhow!("no raydium pool for pair"))?;

        // Check if pool has Serum accounts populated
        let pool = self
            .pools
            .get(&pool_addr)
            .ok_or_else(|| anyhow!("pool snapshot missing"))?;

        // If Serum accounts are available, build full instruction
        // Otherwise return error (caller should have fetched them already)
        if pool.serum_bids.is_none()
            || pool.serum_asks.is_none()
            || pool.serum_event_queue.is_none()
            || pool.serum_base_vault.is_none()
            || pool.serum_quote_vault.is_none()
        {
            return Err(anyhow!(
                "serum market accounts not populated for pool {} - fetch them first",
                pool_addr
            ));
        }

        // Build SerumMarketAccounts from pool data
        let serum = SerumMarketAccounts {
            bids: pool.serum_bids.unwrap(),
            asks: pool.serum_asks.unwrap(),
            event_queue: pool.serum_event_queue.unwrap(),
            base_vault: pool.serum_base_vault.unwrap(),
            quote_vault: pool.serum_quote_vault.unwrap(),
        };

        // Use placeholder user accounts (will be replaced by actual caller)
        let user_authority = Pubkey::default();
        let user_source = Pubkey::default();
        let user_destination = Pubkey::default();
        let serum_program = Pubkey::from_str(OPENBOOK_V3)?;
        let token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")?;
        let rent_sysvar = solana_sdk::sysvar::rent::id();

        // Build full instruction
        let ix = self.build_swap_instruction(
            pool_addr,
            in_pk,
            out_pk,
            amount_in,
            min_out,
            user_authority,
            user_source,
            user_destination,
            serum_program,
            token_program,
            rent_sysvar,
            serum,
            pool.target_orders,
        )?;

        Ok(vec![ix])
    }

    // (build_swap_instruction moved to inherent impl below)

    fn list_pairs(&self) -> Vec<(String, String)> {
        // Performance optimization: Filter out low-liquidity pools (<1 SOL total reserves)
        // This reduces arbitrage scan workload by ~80% with minimal missed opportunities
        const MIN_LIQUIDITY_LAMPORTS: u128 = 1_000_000_000; // 1 SOL = 1B lamports

        self.pools
            .iter()
            .filter(|p| {
                // If reserves are loaded (non-zero), check liquidity threshold
                // If reserves are 0 (lazy-loaded), include pool (will be checked later)
                let total_reserves = p.reserve_base + p.reserve_quote;
                total_reserves == 0 || total_reserves >= MIN_LIQUIDITY_LAMPORTS
            })
            .map(|p| (p.base_mint.to_string(), p.quote_mint.to_string()))
            .collect()
    }
}

impl Raydium {
    /// Attempt to parse essential Serum/OpenBook v3 market account pointers with sanity checks.
    /// We purposely avoid full layout replication; instead we maintain a small set of known offset templates
    /// (there have been minor layout variants historically). Each template lists offsets for:
    /// bids, asks, event_queue, base_vault, quote_vault. We validate:
    ///  - account length large enough for highest offset + 32
    ///  - extracted pubkeys non-default
    ///  - bids != asks, vaults distinct
    ///    If multiple templates match we return the first.
    #[allow(clippy::type_complexity)]
    fn parse_serum_market_accounts(
        data: &[u8],
    ) -> Option<(
        Option<Pubkey>,
        Option<Pubkey>,
        Option<Pubkey>,
        Option<Pubkey>,
        Option<Pubkey>,
    )> {
        #[derive(Clone, Copy)]
        struct Offs {
            bids: usize,
            asks: usize,
            event_q: usize,
            base_vault: usize,
            quote_vault: usize,
        }
        // Standard OpenBook V3 (12 bytes padding: 5 "serum" + 7 pad)
        const T1: Offs = Offs {
            bids: 292,
            asks: 324,
            event_q: 260,
            base_vault: 124,
            quote_vault: 172,
        };
        // Fallback (5 bytes padding: "serum")
        const T2: Offs = Offs {
            bids: 285,
            asks: 317,
            event_q: 253,
            base_vault: 117,
            quote_vault: 165,
        };
        let templates = [T1, T2];
        for t in templates.iter() {
            let req = [t.bids, t.asks, t.event_q, t.base_vault, t.quote_vault]
                .iter()
                .max()
                .cloned()
                .unwrap_or(0)
                + 32;
            if data.len() < req {
                continue;
            }
            let rd = |off: usize| -> Option<Pubkey> {
                let slice = data.get(off..off + 32)?;
                let arr: [u8; 32] = slice.try_into().ok()?;
                let pk = Pubkey::new_from_array(arr);
                if pk.to_bytes() == [0u8; 32] {
                    None
                } else {
                    Some(pk)
                }
            };
            let bids = rd(t.bids);
            let asks = rd(t.asks);
            let event_q = rd(t.event_q);
            let base_vault = rd(t.base_vault);
            let quote_vault = rd(t.quote_vault);
            // Basic sanity
            if bids.is_none()
                || asks.is_none()
                || event_q.is_none()
                || base_vault.is_none()
                || quote_vault.is_none()
            {
                continue;
            }
            if bids == asks {
                continue;
            }
            if base_vault == quote_vault {
                continue;
            }
            return Some((bids, asks, event_q, base_vault, quote_vault));
        }
        None
    }

    /// Fetch Serum market account data from RPC and populate pool's Serum fields.
    /// This is called on-demand when a pool needs Serum accounts for swap execution.
    pub async fn fetch_and_populate_serum_accounts(&self, pool_address: &Pubkey) -> Result<()> {
        // Get pool from cache
        let market_id = {
            let pool = self
                .pools
                .get(pool_address)
                .ok_or_else(|| anyhow!("pool not found in cache"))?;
            pool.market_id
                .ok_or_else(|| anyhow!("pool has no market_id"))?
        };

        // Fetch Serum market account
        let account = self
            .rpc
            .get_account_retry(&market_id)
            .await
            .map_err(|e| anyhow!("failed to fetch serum market account: {}", e))?;

        // Parse Serum market accounts
        let (bids, asks, event_queue, base_vault, quote_vault) =
            Self::parse_serum_market_accounts(&account.data)
                .ok_or_else(|| anyhow!("failed to parse serum market account data"))?;

        // Update pool with Serum accounts
        if let Some(mut pool) = self.pools.get_mut(pool_address) {
            pool.serum_bids = bids;
            pool.serum_asks = asks;
            pool.serum_event_queue = event_queue;
            pool.serum_base_vault = base_vault;
            pool.serum_quote_vault = quote_vault;
            tracing::info!(
                pool=%pool_address,
                market=%market_id,
                bids=%bids.unwrap_or_default(),
                asks=%asks.unwrap_or_default(),
                "fetched and populated serum market accounts"
            );
        }

        Ok(())
    }

    /// Fetch pool vault reserves from RPC and update cache.
    /// This is called on-demand when a pool needs reserves for swap calculation.
    pub async fn fetch_and_update_reserves(&self, pool_address: &Pubkey) -> Result<()> {
        let (base_vault, quote_vault) = {
            let pool = self
                .pools
                .get(pool_address)
                .ok_or_else(|| anyhow!("pool not found in cache"))?;
            (pool.base_vault, pool.quote_vault)
        };

        // Fetch both vault balances in parallel
        let (base_bal, quote_bal) = tokio::try_join!(
            self.rpc.rpc.get_token_account_balance(&base_vault),
            self.rpc.rpc.get_token_account_balance(&quote_vault)
        )
        .map_err(|e| anyhow!("failed to fetch vault balances: {}", e))?;

        let base_amt = base_bal
            .amount
            .parse::<u128>()
            .map_err(|e| anyhow!("failed to parse base amount: {}", e))?;
        let quote_amt = quote_bal
            .amount
            .parse::<u128>()
            .map_err(|e| anyhow!("failed to parse quote amount: {}", e))?;

        if let Some(mut pool) = self.pools.get_mut(pool_address) {
            pool.reserve_base = base_amt;
            pool.reserve_quote = quote_amt;
            tracing::info!(
                pool=%pool_address,
                base=%base_amt,
                quote=%quote_amt,
                "fetched and updated pool reserves"
            );
        }

        Ok(())
    }
}

impl Raydium {
    #[allow(dead_code)]
    pub fn apply_slippage_min_out(quote_amount_out: u64, slippage_bps: u32) -> u64 {
        if slippage_bps == 0 {
            return quote_amount_out;
        }
        let keep = 10_000u64.saturating_sub(slippage_bps as u64);
        (quote_amount_out as u128 * keep as u128 / 10_000u128) as u64
    }

    #[allow(dead_code)]
    pub fn compute_min_out_from_quote(q: &Quote, slippage_bps: u32) -> u64 {
        Self::apply_slippage_min_out(q.amount_out, slippage_bps)
    }

    /// Build a full Raydium swap (base in) using explicit account list. This bypasses internal pool inference.
    #[allow(dead_code)]
    pub fn build_full_swap(
        accounts: RaydiumSwapAccounts,
        amount_in: u64,
        min_out: u64,
        direction_forward: bool,
    ) -> Instruction {
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
                AM {
                    pubkey: accounts.amm_id,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.amm_authority,
                    is_signer: false,
                    is_writable: false,
                },
                AM {
                    pubkey: accounts.amm_open_orders,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.amm_target_orders,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.pool_base_vault,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.pool_quote_vault,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.serum_market,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.serum_bids,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.serum_asks,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.serum_event_queue,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.serum_base_vault,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.serum_quote_vault,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.serum_vault_signer,
                    is_signer: false,
                    is_writable: false,
                },
                AM {
                    pubkey: accounts.user_source,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.user_destination,
                    is_signer: false,
                    is_writable: true,
                },
                AM {
                    pubkey: accounts.user_authority,
                    is_signer: true,
                    is_writable: false,
                },
                AM {
                    pubkey: accounts.token_program,
                    is_signer: false,
                    is_writable: false,
                },
                AM {
                    pubkey: accounts.rent_sysvar,
                    is_signer: false,
                    is_writable: false,
                },
                AM {
                    pubkey: accounts.serum_program,
                    is_signer: false,
                    is_writable: false,
                },
            ],
            data,
        }
    }
}

impl Raydium {
    /// Build a full Raydium swap instruction (BaseIn) using pool snapshot + explicit Serum + user accounts.
    #[allow(clippy::too_many_arguments)]
    pub fn build_swap_instruction(
        &self,
        pool: Pubkey,
        input_mint: Pubkey,
        output_mint: Pubkey,
        amount_in: u64,
        min_out: u64,
        user_authority: Pubkey,
        user_source: Pubkey,
        user_destination: Pubkey,
        serum_program: Pubkey,
        token_program: Pubkey,
        rent_sysvar: Pubkey,
        serum: SerumMarketAccounts,
        amm_target_orders: Option<Pubkey>,
    ) -> Result<Instruction> {
        use solana_sdk::instruction::AccountMeta as AM;
        let snap = self
            .pools
            .get(&pool)
            .ok_or_else(|| anyhow!("pool snapshot missing"))?;
        let forward = snap.base_mint == input_mint && snap.quote_mint == output_mint;
        let reverse = snap.base_mint == output_mint && snap.quote_mint == input_mint;
        if !forward && !reverse {
            return Err(anyhow!("pool does not match provided mints"));
        }
        let direction_forward = forward;
        let amm_authority = snap
            .amm_authority
            .ok_or_else(|| anyhow!("amm_authority missing in snapshot"))?;
        let open_orders = snap
            .open_orders
            .ok_or_else(|| anyhow!("open_orders missing in snapshot"))?;
        let market_id = snap
            .market_id
            .ok_or_else(|| anyhow!("market_id missing in snapshot"))?;
        let _market_program = snap
            .market_program_id
            .ok_or_else(|| anyhow!("market_program_id missing in snapshot"))?; // not directly used in this minimal variant
        let serum_vault_signer = snap
            .serum_vault_signer
            .ok_or_else(|| anyhow!("serum_vault_signer missing in snapshot"))?;
        let target_orders = amm_target_orders.or(snap.target_orders).unwrap_or_default();
        let base_vault = snap.base_vault; // already present
        let quote_vault = snap.quote_vault;
        let mut data = Vec::with_capacity(1 + 8 + 8 + 1);
        data.push(9u8);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());
        data.push(if direction_forward { 0 } else { 1 });
        Ok(Instruction {
            program_id: Self::program_id(),
            accounts: vec![
                // 1. Token Program
                AM {
                    pubkey: token_program,
                    is_signer: false,
                    is_writable: false,
                },
                // 2. AMM ID
                AM {
                    pubkey: pool,
                    is_signer: false,
                    is_writable: true,
                },
                // 3. AMM Authority
                AM {
                    pubkey: amm_authority,
                    is_signer: false,
                    is_writable: false,
                },
                // 4. AMM Open Orders
                AM {
                    pubkey: open_orders,
                    is_signer: false,
                    is_writable: true,
                },
                // 5. AMM Target Orders
                AM {
                    pubkey: target_orders,
                    is_signer: false,
                    is_writable: true,
                },
                // 6. Pool Base Vault
                AM {
                    pubkey: base_vault,
                    is_signer: false,
                    is_writable: true,
                },
                // 7. Pool Quote Vault
                AM {
                    pubkey: quote_vault,
                    is_signer: false,
                    is_writable: true,
                },
                // 8. Serum Program
                AM {
                    pubkey: serum_program,
                    is_signer: false,
                    is_writable: false,
                },
                // 9. Serum Market
                AM {
                    pubkey: market_id,
                    is_signer: false,
                    is_writable: true,
                },
                // 10. Serum Bids
                AM {
                    pubkey: serum.bids,
                    is_signer: false,
                    is_writable: true,
                },
                // 11. Serum Asks
                AM {
                    pubkey: serum.asks,
                    is_signer: false,
                    is_writable: true,
                },
                // 12. Serum Event Queue
                AM {
                    pubkey: serum.event_queue,
                    is_signer: false,
                    is_writable: true,
                },
                // 13. Serum Base Vault
                AM {
                    pubkey: serum.base_vault,
                    is_signer: false,
                    is_writable: true,
                },
                // 14. Serum Quote Vault
                AM {
                    pubkey: serum.quote_vault,
                    is_signer: false,
                    is_writable: true,
                },
                // 15. Serum Vault Signer
                AM {
                    pubkey: serum_vault_signer,
                    is_signer: false,
                    is_writable: false,
                },
                // 16. User Source Token Account
                AM {
                    pubkey: user_source,
                    is_signer: false,
                    is_writable: true,
                },
                // 17. User Destination Token Account
                AM {
                    pubkey: user_destination,
                    is_signer: false,
                    is_writable: true,
                },
                // 18. User Owner (Signer)
                AM {
                    pubkey: user_authority,
                    is_signer: true,
                    is_writable: false,
                },
            ],
            data,
        })
    }
}

/// --- On-chain pool reader subset (blocking client for CLI) ---
pub mod reader {
    use crate::backtest::replay_rpc::ReplayRpc;
    use anyhow::{anyhow, Result};
    use solana_account_decoder::UiAccountEncoding; // Needed for Base64 account encoding switch
    use solana_client::rpc_client::RpcClient;
    use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
    use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
    use solana_sdk::{account::Account, pubkey::Pubkey};
    use std::str::FromStr;

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
        pub const TARGET_ORDERS: usize = 592;
        pub const LP_RESERVE: usize = 720; // u64 LE
        pub const STATUS: usize = 0; // u64 LE
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
        pub target_orders: Option<Pubkey>,
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
                let bytes: &[u8; 32] = data[o..o + 32]
                    .try_into()
                    .map_err(|_| anyhow!("slice -> [u8;32] failed at {o}"))?;
                Ok(Pubkey::new_from_array(*bytes))
            };
            let read_u64 = |o: usize| -> Result<u64> {
                let arr: [u8; 8] = data[o..o + 8]
                    .try_into()
                    .map_err(|_| anyhow!("slice -> [u8;8] failed at {o}"))?;
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
                target_orders: {
                    let pk = read_pk(offs::TARGET_ORDERS)?;
                    if pk.to_bytes() == [0u8; 32] {
                        None
                    } else {
                        Some(pk)
                    }
                },
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
        let mut filters: Vec<RpcFilterType> =
            vec![RpcFilterType::DataSize(LIQ_STATE_V4_SIZE as u64)];
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
            encoding: Some(UiAccountEncoding::Base64),
            data_slice: None,
            commitment: None, // use node default
            min_context_slot: None,
        };

        let cfg = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: acc_cfg,
            with_context: None,
            sort_results: None, // new in Agave 2.x
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

    /// Replay-mode variant: scan all latest accounts from ReplayRpc, filter by size and optional base/quote, and decode pools.
    pub fn fetch_pools_replay(
        replay: &ReplayRpc,
        base: Option<Pubkey>,
        quote: Option<Pubkey>,
        active_only: bool,
        with_accounts: bool,
        _program_id: Pubkey,
    ) -> Result<Vec<PoolV4>> {
        let mut out = Vec::new();
        let items = replay.all_latest();
        for (addr_str, bytes) in items {
            if bytes.len() != LIQ_STATE_V4_SIZE {
                continue;
            }
            // Attempt to parse pool
            let addr = match Pubkey::from_str(&addr_str) {
                Ok(pk) => pk,
                Err(_) => continue,
            };
            let mut d = match PoolV4::decode(addr, &bytes) {
                Ok(p) => p,
                Err(_) => continue,
            };
            // Filter by base/quote if requested
            if let Some(b) = base {
                if d.base_mint != b {
                    continue;
                }
            }
            if let Some(q) = quote {
                if d.quote_mint != q {
                    continue;
                }
            }
            if active_only {
                // Status field at offs::STATUS equals 6 for active; quickly check raw bytes
                if u64::from_le_bytes(
                    bytes[offs::STATUS..offs::STATUS + 8]
                        .try_into()
                        .unwrap_or([0u8; 8]),
                ) != 6
                {
                    continue;
                }
            }
            if with_accounts {
                d.raw_account = Some(Account {
                    lamports: 0,
                    data: bytes.clone(),
                    owner: Pubkey::default(),
                    executable: false,
                    rent_epoch: 0,
                });
            }
            out.push(d);
        }
        Ok(out)
    }
}
