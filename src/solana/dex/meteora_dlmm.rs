//! Meteora DLMM (Dynamic Liquidity Market Maker) DEX Connector
//!
//! Implements the Dex trait for Meteora DLMM pools with basic constant product approximation.
//! Full bin-walking quote calculation will be implemented in Phase 2.

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use dashmap::DashMap;
#[cfg(feature = "rpc_fallback")]
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
#[cfg(feature = "rpc_fallback")]
use solana_client::rpc_filter::RpcFilterType;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tracing::debug;
#[cfg(feature = "rpc_fallback")]
use tracing::warn;

use super::meteora_dlmm_layout::DlmmPool;
use super::meteora_swap_builder::{MeteoraDlmmSwapBuilder, SwapDirection};
use super::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;

/// Meteora DLMM Program ID
pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

/// LB Pair account size (904 bytes)
#[cfg(feature = "rpc_fallback")]
const LB_PAIR_ACCOUNT_SIZE: usize = 904;

/// Cached pool state
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct PoolCache {
    address: Pubkey,
    pool: DlmmPool,
    /// Reserve balances fetched from vaults (cached)
    reserve_x_balance: Option<u64>,
    reserve_y_balance: Option<u64>,
    last_updated: std::time::SystemTime,
}

pub struct MeteoraDlmm {
    rpc: Arc<SolanaRpc>,
    pools: Arc<DashMap<Pubkey, PoolCache>>,
    mint_index: Arc<DashMap<Pubkey, Vec<Pubkey>>>, // mint -> pool addresses
    user_authority: parking_lot::RwLock<Option<Pubkey>>,
}

impl MeteoraDlmm {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self {
            rpc,
            pools: Arc::new(DashMap::new()),
            mint_index: Arc::new(DashMap::new()),
            user_authority: parking_lot::RwLock::new(None),
        }
    }

    /// Set the user authority (wallet pubkey) for building swap instructions
    pub fn set_user_authority(&mut self, auth: Pubkey) {
        *self.user_authority.write() = Some(auth);
    }

    /// Get pool snapshot for a specific address
    pub fn get_pool(&self, address: &Pubkey) -> Option<DlmmPool> {
        self.pools.get(address).map(|entry| entry.pool.clone())
    }

    /// List all tracked pool addresses
    pub fn list_pools(&self) -> Vec<Pubkey> {
        self.pools.iter().map(|entry| *entry.key()).collect()
    }

    /// Find pools containing a specific mint
    pub fn pools_for_mint(&self, mint: &Pubkey) -> Vec<Pubkey> {
        self.mint_index
            .get(mint)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    // REMOVED: discover_pool_on_demand()
    //
    // **ARCHITECTURE COMPLIANCE (TARGET_ARCHITECTURE.md Section 4.2):**
    // Pool Discovery belongs ONLY in market-data (Data Plane) via Geyser.
    // DEX Connectors in execution-engine must NOT do getProgramAccounts()
    // or any RPC-based pool discovery.
    //
    // If a pool is not in cache, it means market-data hasn't discovered it yet.
    // The intent should be rejected with reason_code="pool_not_cached".

    /// Fetch reserve balances from vaults and update cache
    ///
    /// NOTE: This is used for IX building when pool is already in cache.
    /// In production, reserve data should come from Geyser via intent metadata.
    async fn update_reserve_balances(&self, pool_addr: &Pubkey) -> Result<(u64, u64)> {
        let pool = self
            .pools
            .get(pool_addr)
            .ok_or_else(|| anyhow!("Pool not found: {}", pool_addr))?;

        let reserve_x = pool.pool.reserve_x;
        let reserve_y = pool.pool.reserve_y;
        drop(pool);

        // Fetch token account balances
        let acc_x = self.rpc.get_account_retry(&reserve_x).await.ok();
        let acc_y = self.rpc.get_account_retry(&reserve_y).await.ok();

        let balance_x = acc_x
            .as_ref()
            .and_then(|acc| {
                if acc.data.len() >= 72 {
                    Some(u64::from_le_bytes(acc.data[64..72].try_into().ok()?))
                } else {
                    None
                }
            })
            .unwrap_or(0);

        let balance_y = acc_y
            .as_ref()
            .and_then(|acc| {
                if acc.data.len() >= 72 {
                    Some(u64::from_le_bytes(acc.data[64..72].try_into().ok()?))
                } else {
                    None
                }
            })
            .unwrap_or(0);

        // Update cache
        if let Some(mut entry) = self.pools.get_mut(pool_addr) {
            entry.reserve_x_balance = Some(balance_x);
            entry.reserve_y_balance = Some(balance_y);
            entry.last_updated = std::time::SystemTime::now();
        }

        Ok((balance_x, balance_y))
    }

    /// Simple constant product quote approximation (Phase 1)
    /// TODO: Implement proper bin-walking algorithm in Phase 2
    fn quote_constant_product(
        &self,
        reserve_in: u64,
        reserve_out: u64,
        amount_in: u64,
        fee_bps: u32,
    ) -> Result<Quote> {
        ensure!(reserve_in > 0 && reserve_out > 0, "Empty reserves");
        ensure!(amount_in > 0, "Amount must be positive");

        // Apply fee (fee_bps is typically 30-100 for DLMM)
        let fee_amount = (amount_in as u128 * fee_bps as u128) / 10000;
        let amount_in_after_fee = amount_in as u128 - fee_amount;

        // Constant product: x * y = k
        let reserve_in_u128 = reserve_in as u128;
        let reserve_out_u128 = reserve_out as u128;

        let amount_out =
            (amount_in_after_fee * reserve_out_u128) / (reserve_in_u128 + amount_in_after_fee);

        ensure!(
            amount_out < u64::MAX as u128,
            "Amount out overflow: {}",
            amount_out
        );

        // Price impact calculation
        let price_before = (reserve_out_u128 * 10000) / reserve_in_u128;
        let new_reserve_in = reserve_in_u128 + amount_in_after_fee;
        let new_reserve_out = reserve_out_u128 - amount_out;
        let price_after = if new_reserve_out > 0 {
            (new_reserve_out * 10000) / new_reserve_in
        } else {
            0
        };

        let price_impact_bps = if price_after < price_before {
            ((price_before - price_after) * 10000 / price_before) as u32
        } else {
            0
        };

        Ok(Quote {
            amount_out: amount_out as u64,
            price_impact_bps,
            route: vec![],
            fee_bps,
            in_reserve: reserve_in_u128,
            out_reserve: reserve_out_u128,
            input_mint: String::new(),
            output_mint: String::new(),
            tick_spacing: None,
        })
    }
}

#[async_trait]
impl Dex for MeteoraDlmm {
    /// Refresh pool cache via RPC getProgramAccounts.
    ///
    /// ⚠️ **RPC FALLBACK ONLY** - Use Geyser-based pool discovery in production!
    ///
    /// This method exists for:
    /// - Bootstrap/initialization when Geyser is not yet available
    /// - Testing and development
    /// - Fallback when Geyser stream is interrupted
    ///
    /// In production, pool discovery should happen via `GeyserPoolDiscovery`
    /// which provides real-time pool updates without expensive RPC scans.
    ///
    /// See: docs/TARGET_ARCHITECTURE.md - "Geyser preferred, RPC only as fallback"
    ///
    /// **Feature-gated:** Only available with `rpc_fallback` feature.
    /// In production builds without this feature, returns Ok(()) immediately.
    async fn refresh_pools(&self) -> Result<()> {
        #[cfg(not(feature = "rpc_fallback"))]
        {
            debug!("refresh_pools() disabled - rpc_fallback feature not enabled");
            return Ok(());
        }

        #[cfg(feature = "rpc_fallback")]
        {
        debug!("Fetching Meteora DLMM pools via getProgramAccounts");

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;

        // Filter for LB Pair accounts (904 bytes)
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::DataSize(LB_PAIR_ACCOUNT_SIZE as u64)]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                data_slice: None,
                commitment: None,
                min_context_slot: None,
            },
            with_context: None,
            sort_results: None,
        };

        let accounts = self
            .rpc
            .get_program_accounts_with_config_retry(&program_id, config)
            .await?;

        debug!("Found {} Meteora DLMM pools", accounts.len());

        for (pubkey, account) in accounts {
            match DlmmPool::parse(&account.data) {
                Ok(pool) => {
                    let cache = PoolCache {
                        address: pubkey,
                        pool: pool.clone(),
                        reserve_x_balance: None,
                        reserve_y_balance: None,
                        last_updated: std::time::SystemTime::now(),
                    };

                    self.pools.insert(pubkey, cache);

                    // Update mint index
                    self.mint_index
                        .entry(pool.token_x_mint)
                        .or_default()
                        .push(pubkey);

                    self.mint_index
                        .entry(pool.token_y_mint)
                        .or_default()
                        .push(pubkey);

                    debug!(
                        "Loaded DLMM pool {}: {}/{} (bin_step={})",
                        pubkey, pool.token_x_mint, pool.token_y_mint, pool.bin_step
                    );
                }
                Err(e) => {
                    warn!("Failed to parse DLMM pool {}: {}", pubkey, e);
                }
            }
        }

        debug!(
            "Meteora DLMM refresh complete: {} pools, {} mints",
            self.pools.len(),
            self.mint_index.len()
        );

        Ok(())
        } // end #[cfg(feature = "rpc_fallback")]
    }

    /// Load a single pool by address via getAccount RPC.
    ///
    /// **ARCHITECTURE COMPLIANCE (TARGET_ARCHITECTURE.md Section 4.2):**
    /// This is a single getAccount call (acceptable) to pre-load a pool
    /// that arb-strategy discovered and passed via Intent metadata.
    /// NOT getProgramAccounts - no scanning.
    async fn load_pool_by_address(&self, pool_address: &Pubkey) -> Result<()> {
        // Check if already cached
        if self.pools.contains_key(pool_address) {
            debug!(
                "Meteora DLMM pool {} already in cache",
                pool_address
            );
            return Ok(());
        }

        debug!(
            "Loading Meteora DLMM pool {} via single getAccount",
            pool_address
        );

        // Fetch account data
        let account = self
            .rpc
            .get_account_retry(pool_address)
            .await
            .map_err(|e| anyhow!("Failed to fetch DLMM pool {}: {}", pool_address, e))?;

        // Parse pool
        let pool = DlmmPool::parse(&account.data)
            .map_err(|e| anyhow!("Failed to parse DLMM pool {}: {}", pool_address, e))?;

        // Insert into cache
        let cache = PoolCache {
            address: *pool_address,
            pool: pool.clone(),
            reserve_x_balance: None,
            reserve_y_balance: None,
            last_updated: std::time::SystemTime::now(),
        };

        self.pools.insert(*pool_address, cache);

        // Update mint index
        self.mint_index
            .entry(pool.token_x_mint)
            .or_default()
            .push(*pool_address);
        self.mint_index
            .entry(pool.token_y_mint)
            .or_default()
            .push(*pool_address);

        debug!(
            "Loaded Meteora DLMM pool {}: {}/{} (bin_step={}) via single RPC call",
            pool_address, pool.token_x_mint, pool.token_y_mint, pool.bin_step
        );

        Ok(())
    }

    /// Set pool data directly from accounts list (NO RPC calls).
    ///
    /// This is the preferred method for execution-engine. The accounts come from
    /// `Intent.resources.accounts` which were populated by arb-strategy from
    /// `DexPoolAccounts` events (Geyser data).
    ///
    /// Expected accounts format (from market_data.rs DexPoolAccounts for Meteora DLMM):
    /// - accounts[0] = pool_address (lb_pair)
    /// - accounts[1] = token_x_mint (in original lb_pair order, NOT semantically "base")
    /// - accounts[2] = token_y_mint (in original lb_pair order, NOT semantically "quote")
    /// - accounts[3] = reserve_x (token_x vault)
    /// - accounts[4] = reserve_y (token_y vault)
    /// - accounts[5+] = tagged values: "active_id:<value>", "bin_step:<value>"
    ///
    /// Note: This creates a minimal pool entry for IX building.
    fn set_pool_from_accounts(&self, pool_address: &str, accounts: &[String]) -> Result<()> {
        // Minimum required: pool_address, token_x_mint, token_y_mint
        if accounts.len() < 3 {
            return Err(anyhow!(
                "meteora_dlmm set_pool_from_accounts requires at least 3 accounts, got {}",
                accounts.len()
            ));
        }

        let parse_pubkey = |s: &str, name: &str| -> Result<Pubkey> {
            Pubkey::from_str(s).map_err(|e| anyhow!("Invalid {} pubkey '{}': {}", name, s, e))
        };

        let pool_pk = parse_pubkey(pool_address, "pool_address")?;
        
        // accounts[1] and accounts[2] are token_x_mint and token_y_mint in ORIGINAL lb_pair order
        let token_x_mint = parse_pubkey(&accounts[1], "token_x_mint")?;
        let token_y_mint = parse_pubkey(&accounts[2], "token_y_mint")?;

        // Reserve vaults: reserve_x is token_x vault, reserve_y is token_y vault
        let reserve_x = if accounts.len() > 3 && !accounts[3].contains(':') {
            parse_pubkey(&accounts[3], "reserve_x")?
        } else {
            // Use placeholder - IX building will fail if actual vault needed
            Pubkey::default()
        };

        let reserve_y = if accounts.len() > 4 && !accounts[4].contains(':') {
            parse_pubkey(&accounts[4], "reserve_y")?
        } else {
            Pubkey::default()
        };

        // Validate pool_address matches accounts[0]
        let expected_pool = parse_pubkey(&accounts[0], "accounts[0]")?;
        if pool_pk != expected_pool {
            return Err(anyhow!(
                "pool_address {} does not match accounts[0] {}",
                pool_address,
                expected_pool
            ));
        }

        // Parse tagged values (active_id, bin_step) from remaining accounts
        let mut active_id: i32 = 0;
        let mut bin_step: u16 = 10; // Default
        
        for account in accounts.iter().skip(3) {
            if let Some(value) = account.strip_prefix("active_id:") {
                if let Ok(v) = value.parse::<i32>() {
                    active_id = v;
                }
            } else if let Some(value) = account.strip_prefix("bin_step:") {
                if let Ok(v) = value.parse::<u16>() {
                    bin_step = v;
                }
            }
        }

        // Create DlmmPool structure with actual values from DexPoolAccounts
        let pool = DlmmPool {
            discriminator: [0u8; 8], // Not needed for IX building
            bin_step,
            active_id,
            token_x_mint,
            token_y_mint,
            reserve_x,
            reserve_y,
        };

        let cache = PoolCache {
            address: pool_pk,
            pool: pool.clone(),
            reserve_x_balance: None,
            reserve_y_balance: None,
            last_updated: std::time::SystemTime::now(),
        };

        debug!(
            pool = %pool_pk,
            token_x = %token_x_mint,
            token_y = %token_y_mint,
            active_id = active_id,
            bin_step = bin_step,
            "meteora_dlmm pool set from intent accounts (NO RPC)"
        );

        self.pools.insert(pool_pk, cache);

        // Update mint index
        self.mint_index
            .entry(token_x_mint)
            .or_default()
            .push(pool_pk);
        self.mint_index
            .entry(token_y_mint)
            .or_default()
            .push(pool_pk);

        Ok(())
    }

    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>> {
        let input_pk = Pubkey::from_str(input_mint)?;
        let output_pk = Pubkey::from_str(output_mint)?;

        // Find pools with both mints from cache
        let input_pools = self.pools_for_mint(&input_pk);
        let output_pools = self.pools_for_mint(&output_pk);

        let matching_pools: Vec<_> = input_pools
            .iter()
            .filter(|p| output_pools.contains(p))
            .copied()
            .collect();

        // NO on-demand discovery! Per TARGET_ARCHITECTURE.md Section 4.2:
        // Pool Discovery belongs ONLY in market-data via Geyser.
        // If no cached pool, the quote fails (intent will be rejected with pool_not_cached).
        if matching_pools.is_empty() {
            debug!(
                "No cached DLMM pool for {}/{} - pool discovery must happen in market-data",
                input_mint, output_mint
            );
            return Ok(None);
        }

        // Use first matching pool (TODO: multi-pool routing in Phase 3)
        let pool_addr = matching_pools[0];

        let pool = self
            .pools
            .get(&pool_addr)
            .ok_or_else(|| anyhow!("Pool disappeared"))?;

        // Determine direction (X->Y or Y->X)
        let is_x_to_y = pool.pool.token_x_mint == input_pk;

        // Fetch reserve balances
        drop(pool);
        let (balance_x, balance_y) = self.update_reserve_balances(&pool_addr).await?;

        let (reserve_in, reserve_out) = if is_x_to_y {
            (balance_x, balance_y)
        } else {
            (balance_y, balance_x)
        };

        // Use fee from bin_step (bin_step in bps = fee approximation for now)
        // TODO: Get actual fee from pool parameters
        let fee_bps = 30; // Conservative default, actual fee varies by pool

        let mut quote = self.quote_constant_product(reserve_in, reserve_out, amount_in, fee_bps)?;

        // Fill in metadata
        quote.input_mint = input_mint.to_string();
        quote.output_mint = output_mint.to_string();
        quote.route = vec![pool_addr.to_string()];

        Ok(Some(quote))
    }

    fn build_swap_ix(
        &self,
        _input_mint: &str,
        _output_mint: &str,
        _amount_in: u64,
        _min_out: u64,
    ) -> Result<Vec<Instruction>> {
        // Meteora DLMM requires async build (for bin array fetching).
        // Use build_swap_ix_async instead.
        Err(anyhow!(
            "meteora_dlmm: use build_swap_ix_async (requires bin array fetching)"
        ))
    }

    /// Async version that properly fetches bin arrays.
    ///
    /// Meteora DLMM swaps require multiple bin_array accounts as "remaining accounts".
    /// The exact bin arrays needed depend on the active_id and swap size.
    /// This async method uses `build_swap_with_bins()` which fetches the correct
    /// bin arrays from chain.
    ///
    /// NOTE: Pool data (active_id, bin_step) comes from DexPoolAccounts event
    /// which is populated by market-data from Geyser. No RPC calls in hot path.
    async fn build_swap_ix_async(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>> {
        let input_mint_pk = Pubkey::from_str(input_mint)?;
        let output_mint_pk = Pubkey::from_str(output_mint)?;

        // Find pool for this pair
        let pool_entry = self
            .pools
            .iter()
            .find(|entry| {
                let pool = &entry.value().pool;
                (pool.token_x_mint == input_mint_pk && pool.token_y_mint == output_mint_pk)
                    || (pool.token_x_mint == output_mint_pk && pool.token_y_mint == input_mint_pk)
            })
            .ok_or_else(|| anyhow!("No pool found for {}/{}", input_mint, output_mint))?;

        let pool_addr = *pool_entry.key();
        let pool = pool_entry.value().pool.clone();
        drop(pool_entry); // Release DashMap lock before async call

        // Validate that we have real active_id/bin_step from DexPoolAccounts
        // If active_id is 0 and bin_step is default (10), the Intent may be missing data
        if pool.active_id == 0 && pool.bin_step == 10 {
            debug!(
                pool = %pool_addr,
                active_id = pool.active_id,
                bin_step = pool.bin_step,
                "meteora_dlmm: WARNING - using default active_id/bin_step, Intent may be missing DexPoolAccounts data"
            );
        }

        debug!(
            pool = %pool_addr,
            active_id = pool.active_id,
            bin_step = pool.bin_step,
            token_x = %pool.token_x_mint,
            token_y = %pool.token_y_mint,
            "meteora_dlmm: building swap with cached pool data (no RPC)"
        );

        // Determine swap direction
        let direction = if pool.token_x_mint == input_mint_pk {
            SwapDirection::XtoY
        } else {
            SwapDirection::YtoX
        };

        // Get user authority
        let user = self
            .user_authority
            .read()
            .ok_or_else(|| anyhow!("meteora_dlmm user_authority not set"))?;

        // Convert to spl_token Pubkey type for ATA derivation
        let owner_spl =
            spl_token::solana_program::pubkey::Pubkey::new_from_array(user.to_bytes());
        let token_x_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(
            pool.token_x_mint.to_bytes(),
        );
        let token_y_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(
            pool.token_y_mint.to_bytes(),
        );
        let token_program_spl = spl_token::id();

        // Derive ATAs for user
        let user_token_x_spl =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &owner_spl,
                &token_x_spl,
                &token_program_spl,
            );
        let user_token_y_spl =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &owner_spl,
                &token_y_spl,
                &token_program_spl,
            );

        // Convert back to solana_sdk Pubkey
        let user_token_x = Pubkey::new_from_array(user_token_x_spl.to_bytes());
        let user_token_y = Pubkey::new_from_array(user_token_y_spl.to_bytes());

        // Build swap instruction WITH bin arrays (async)
        // Uses active_id and bin_step from DexPoolAccounts (via set_pool_from_accounts)
        let swap_builder = MeteoraDlmmSwapBuilder::new(self.rpc.clone());
        let ix = swap_builder
            .build_swap_with_bins(
                &pool_addr,
                &pool.reserve_x,
                &pool.reserve_y,
                &user_token_x,
                &user_token_y,
                &pool.token_x_mint,
                &pool.token_y_mint,
                &user,
                amount_in,
                min_out,
                direction,
                pool.active_id,
                pool.bin_step,
            )
            .await?;

        Ok(vec![ix])
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        for entry in self.pools.iter() {
            let pool = &entry.value().pool;
            pairs.push((pool.token_x_mint.to_string(), pool.token_y_mint.to_string()));
        }
        pairs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_product_quote() {
        let meteora = MeteoraDlmm::new(Arc::new(SolanaRpc::new("https://dummy")));

        // 1000 SOL / 100000 USDC pool, swap 1 SOL
        let quote = meteora
            .quote_constant_product(1_000_000_000, 100_000_000_000, 1_000_000, 30)
            .expect("Quote failed");

        // Expected: ~99.7 USDC (with 0.3% fee)
        assert!(quote.amount_out > 99_000_000 && quote.amount_out < 100_000_000);
        assert_eq!(quote.fee_bps, 30);
    }
}
