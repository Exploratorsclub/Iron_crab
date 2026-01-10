//! Meteora DLMM (Dynamic Liquidity Market Maker) DEX Connector
//!
//! Implements the Dex trait for Meteora DLMM pools with basic constant product approximation.
//! Full bin-walking quote calculation will be implemented in Phase 2.

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::RpcFilterType;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, warn};

use super::meteora_dlmm_layout::DlmmPool;
use super::meteora_swap_builder::{MeteoraDlmmSwapBuilder, SwapDirection};
use super::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;

/// Meteora DLMM Program ID
pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

/// LB Pair account size (904 bytes)
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
}

impl MeteoraDlmm {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self {
            rpc,
            pools: Arc::new(DashMap::new()),
            mint_index: Arc::new(DashMap::new()),
        }
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

    /// Discover pool on-demand by fetching account data directly
    ///
    /// This is used when the pool cache is empty (e.g., no refresh_pools() was called).
    /// The method searches for pools containing both mints by scanning the Meteora program.
    async fn discover_pool_on_demand(
        &self,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
    ) -> Result<Option<Pubkey>> {
        use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
        use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};

        let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)?;

        // Try both orderings (X/Y or Y/X)
        for (mint_x, mint_y) in [(input_mint, output_mint), (output_mint, input_mint)] {
            // Filter for accounts with token_x_mint at offset 88 and token_y_mint at offset 120
            let config = RpcProgramAccountsConfig {
                filters: Some(vec![
                    RpcFilterType::DataSize(LB_PAIR_ACCOUNT_SIZE as u64),
                    RpcFilterType::Memcmp(Memcmp::new(
                        88, // token_x_mint offset
                        MemcmpEncodedBytes::Base58(mint_x.to_string()),
                    )),
                    RpcFilterType::Memcmp(Memcmp::new(
                        120, // token_y_mint offset
                        MemcmpEncodedBytes::Base58(mint_y.to_string()),
                    )),
                ]),
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

            if let Some((pubkey, account)) = accounts.into_iter().next() {
                // Parse and cache the pool
                if let Ok(pool) = DlmmPool::parse(&account.data) {
                    debug!(
                        "Discovered DLMM pool on-demand: {} ({}/{})",
                        pubkey, pool.token_x_mint, pool.token_y_mint
                    );

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

                    return Ok(Some(pubkey));
                }
            }
        }

        debug!("No DLMM pool found for {}/{}", input_mint, output_mint);
        Ok(None)
    }

    /// Fetch reserve balances from vaults and update cache
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
    async fn refresh_pools(&self) -> Result<()> {
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

        let mut matching_pools: Vec<_> = input_pools
            .iter()
            .filter(|p| output_pools.contains(p))
            .copied()
            .collect();

        // If no pool in cache, try on-demand discovery
        if matching_pools.is_empty() {
            debug!(
                "No cached DLMM pool for {}/{}, attempting on-demand discovery",
                input_mint, output_mint
            );
            if let Some(pool_addr) = self.discover_pool_on_demand(&input_pk, &output_pk).await? {
                matching_pools.push(pool_addr);
            }
        }

        if matching_pools.is_empty() {
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
        let pool = &pool_entry.value().pool;

        // Determine swap direction
        let direction = if pool.token_x_mint == input_mint_pk {
            SwapDirection::XtoY
        } else {
            SwapDirection::YtoX
        };

        // Build swap instruction
        // Note: This is a simplified implementation. Full version needs:
        // 1. User's actual token accounts (ATAs)
        // 2. Bin array accounts (dynamic based on active_id)
        // 3. Proper authority derivation
        let swap_builder = MeteoraDlmmSwapBuilder::new(self.rpc.clone());

        // Placeholder: In production, these would come from wallet/treasury
        let user = Pubkey::default(); // TODO: Get from caller
        let user_token_x = Pubkey::default(); // TODO: Derive ATA
        let user_token_y = Pubkey::default(); // TODO: Derive ATA

        let ix = swap_builder.build_swap(
            &pool_addr,
            &pool.reserve_x,
            &pool.reserve_y,
            &user_token_x,
            &user_token_y,
            &user,
            amount_in,
            min_out,
            direction,
        )?;

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
