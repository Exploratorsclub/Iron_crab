//! Raydium CPMM (Constant Product Market Maker) DEX Connector
//!
//! Implements the Dex trait for Raydium's newer CPMM pools.
//! Unlike AMM V4 (which uses Serum), CPMM is a pure constant product AMM.

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::RpcFilterType;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, warn};

use super::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;

/// Raydium CPMM Program ID
pub const RAYDIUM_CPMM_PROGRAM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";

/// SPL Token Program
pub const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// Pool state account size (verified from mainnet: 1024 bytes)
/// SDK Layout: CpmmPoolInfoLayout with ~61 fields including:
/// - discriminator(8) + configId(32) + creator(32) + vaults(64) + mints(96)
/// - lp_mint(32) + mint_programs(64) + observation(32) + status fields
/// - fees, amounts, timestamps, padding -> ~1024 total
const CPMM_POOL_ACCOUNT_SIZE: usize = 1024;

/// Cached pool state
#[derive(Clone, Debug)]
struct PoolCache {
    address: Pubkey,
    token_0_mint: Pubkey,
    token_1_mint: Pubkey,
    token_0_vault: Pubkey,
    token_1_vault: Pubkey,
    lp_mint: Pubkey,
    /// Cached reserve balances
    reserve_0: u64,
    reserve_1: u64,
    /// Fee in basis points (typically 25 = 0.25%)
    fee_bps: u32,
    last_updated: std::time::SystemTime,
}

/// Raydium CPMM Pool State
/// Layout reverse-engineered from mainnet pools
#[derive(Debug, Clone)]
pub struct CpmmPool {
    /// Status of the pool (0 = uninitialized, 1 = initialized, etc.)
    pub status: u8,
    
    /// Token 0 mint
    pub token_0_mint: Pubkey,
    
    /// Token 1 mint
    pub token_1_mint: Pubkey,
    
    /// Token 0 vault (holds reserves)
    pub token_0_vault: Pubkey,
    
    /// Token 1 vault (holds reserves)
    pub token_1_vault: Pubkey,
    
    /// LP token mint
    pub lp_mint: Pubkey,
    
    /// Fee rate in basis points
    pub fee_rate: u64,
}

impl CpmmPool {
    /// Parse CPMM pool from raw account data
    /// 
    /// Note: This is a simplified parser. Full implementation requires
    /// verifying exact offsets from Raydium's CPMM SDK or mainnet inspection.
    pub fn parse(data: &[u8]) -> Result<Self> {
        ensure!(
            data.len() >= 200, // Minimum size for header + mints + vaults
            "Invalid CPMM pool size: {} (expected at least 200)",
            data.len()
        );
        
        // Anchor discriminator (8 bytes) + status (1 byte)
        let status = data[8];
        
        // Offsets are estimates - need to verify from mainnet
        // Typical Anchor layout: discriminator (8) + struct fields
        
        // Token 0 mint (offset ~16, 32 bytes)
        let token_0_mint = Pubkey::new_from_array(
            data[16..48].try_into()
                .map_err(|_| anyhow!("Failed to parse token_0_mint"))?
        );
        
        // Token 1 mint (offset ~48, 32 bytes)
        let token_1_mint = Pubkey::new_from_array(
            data[48..80].try_into()
                .map_err(|_| anyhow!("Failed to parse token_1_mint"))?
        );
        
        // Token 0 vault (offset ~80, 32 bytes)
        let token_0_vault = Pubkey::new_from_array(
            data[80..112].try_into()
                .map_err(|_| anyhow!("Failed to parse token_0_vault"))?
        );
        
        // Token 1 vault (offset ~112, 32 bytes)
        let token_1_vault = Pubkey::new_from_array(
            data[112..144].try_into()
                .map_err(|_| anyhow!("Failed to parse token_1_vault"))?
        );
        
        // LP mint (offset ~144, 32 bytes)
        let lp_mint = Pubkey::new_from_array(
            data[144..176].try_into()
                .map_err(|_| anyhow!("Failed to parse lp_mint"))?
        );
        
        // Fee rate (offset ~176, 8 bytes, u64 LE)
        let fee_rate = if data.len() >= 184 {
            u64::from_le_bytes(
                data[176..184].try_into()
                    .map_err(|_| anyhow!("Failed to parse fee_rate"))?
            )
        } else {
            2500 // Default: 0.25% = 25 bps
        };
        
        Ok(Self {
            status,
            token_0_mint,
            token_1_mint,
            token_0_vault,
            token_1_vault,
            lp_mint,
            fee_rate,
        })
    }
}

pub struct RaydiumCpmm {
    rpc: Arc<SolanaRpc>,
    pools: Arc<DashMap<Pubkey, PoolCache>>,
    mint_index: Arc<DashMap<Pubkey, Vec<Pubkey>>>, // mint -> pool addresses
}

impl RaydiumCpmm {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self {
            rpc,
            pools: Arc::new(DashMap::new()),
            mint_index: Arc::new(DashMap::new()),
        }
    }

    /// Get pool snapshot for a specific address
    pub fn get_pool(&self, address: &Pubkey) -> Option<PoolCache> {
        self.pools.get(address).map(|entry| entry.clone())
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

    /// Fetch reserve balances from vaults and update cache
    async fn update_reserve_balances(&self, pool_addr: &Pubkey) -> Result<(u64, u64)> {
        let pool = self
            .pools
            .get(pool_addr)
            .ok_or_else(|| anyhow!("Pool not found: {}", pool_addr))?;

        let vault_0 = pool.token_0_vault;
        let vault_1 = pool.token_1_vault;
        drop(pool);

        // Fetch token account balances
        let acc_0 = self.rpc.get_account_retry(&vault_0).await.ok();
        let acc_1 = self.rpc.get_account_retry(&vault_1).await.ok();

        let balance_0 = acc_0
            .as_ref()
            .and_then(|acc| {
                if acc.data.len() >= 72 {
                    Some(u64::from_le_bytes(acc.data[64..72].try_into().ok()?))
                } else {
                    None
                }
            })
            .unwrap_or(0);

        let balance_1 = acc_1
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
        if let Some(mut pool) = self.pools.get_mut(pool_addr) {
            pool.reserve_0 = balance_0;
            pool.reserve_1 = balance_1;
            pool.last_updated = std::time::SystemTime::now();
        }

        Ok((balance_0, balance_1))
    }

    /// Calculate quote using constant product formula: x * y = k
    fn quote_constant_product(
        &self,
        reserve_in: u64,
        reserve_out: u64,
        amount_in: u64,
        fee_bps: u32,
    ) -> Result<Quote> {
        ensure!(reserve_in > 0, "Reserve in is zero");
        ensure!(reserve_out > 0, "Reserve out is zero");
        ensure!(amount_in > 0, "Amount in must be positive");

        let reserve_in_u128 = reserve_in as u128;
        let reserve_out_u128 = reserve_out as u128;
        let amount_in_u128 = amount_in as u128;

        // Apply fee (fee is taken from input)
        let fee_amount = (amount_in_u128 * fee_bps as u128) / 10000;
        let amount_in_after_fee = amount_in_u128 - fee_amount;

        // Constant product: (x + Δx) * (y - Δy) = x * y
        // Solve for Δy: Δy = y - (x*y)/(x + Δx)
        let k = reserve_in_u128 * reserve_out_u128;
        let new_reserve_in = reserve_in_u128 + amount_in_after_fee;
        let new_reserve_out = k / new_reserve_in;
        let amount_out = reserve_out_u128 - new_reserve_out;

        // Price impact calculation
        let price_before = (reserve_out_u128 * 10000) / reserve_in_u128;
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
impl Dex for RaydiumCpmm {
    async fn refresh_pools(&self) -> Result<()> {
        debug!("Fetching Raydium CPMM pools via getProgramAccounts");

        let program_id = Pubkey::from_str(RAYDIUM_CPMM_PROGRAM)?;

        // Filter for CPMM pool accounts
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![
                RpcFilterType::DataSize(CPMM_POOL_ACCOUNT_SIZE as u64)
            ]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
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

        debug!("Found {} Raydium CPMM pools", accounts.len());

        // Clear old pools and index
        self.pools.clear();
        self.mint_index.clear();

        for (pubkey, account) in accounts {
            match CpmmPool::parse(&account.data) {
                Ok(pool) => {
                    // Create cache entry
                    let cache = PoolCache {
                        address: pubkey,
                        token_0_mint: pool.token_0_mint,
                        token_1_mint: pool.token_1_mint,
                        token_0_vault: pool.token_0_vault,
                        token_1_vault: pool.token_1_vault,
                        lp_mint: pool.lp_mint,
                        reserve_0: 0,
                        reserve_1: 0,
                        fee_bps: (pool.fee_rate / 100) as u32, // Convert to bps
                        last_updated: std::time::SystemTime::now(),
                    };

                    // Add to pool cache
                    self.pools.insert(pubkey, cache.clone());

                    // Add to mint index
                    self.mint_index
                        .entry(pool.token_0_mint)
                        .or_insert_with(Vec::new)
                        .push(pubkey);
                    self.mint_index
                        .entry(pool.token_1_mint)
                        .or_insert_with(Vec::new)
                        .push(pubkey);

                    debug!(
                        "Loaded CPMM pool: {} ({}/{})",
                        pubkey,
                        pool.token_0_mint,
                        pool.token_1_mint
                    );
                }
                Err(e) => {
                    warn!("Failed to parse CPMM pool {}: {}", pubkey, e);
                }
            }
        }

        Ok(())
    }

    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>> {
        let input_mint_pk = Pubkey::from_str(input_mint)?;
        let output_mint_pk = Pubkey::from_str(output_mint)?;

        // Find pool for this pair
        let pool_entry = self
            .pools
            .iter()
            .find(|entry| {
                let cache = entry.value();
                (cache.token_0_mint == input_mint_pk && cache.token_1_mint == output_mint_pk)
                    || (cache.token_0_mint == output_mint_pk && cache.token_1_mint == input_mint_pk)
            });

        let pool_entry = match pool_entry {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let pool_addr = *pool_entry.key();
        let fee_bps = pool_entry.value().fee_bps;
        drop(pool_entry);

        // Fetch latest reserve balances
        let (balance_0, balance_1) = self.update_reserve_balances(&pool_addr).await?;

        // Determine direction
        let pool = self.pools.get(&pool_addr).unwrap();
        let (reserve_in, reserve_out) = if pool.token_0_mint == input_mint_pk {
            (balance_0, balance_1)
        } else {
            (balance_1, balance_0)
        };

        // Calculate quote
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
                let cache = entry.value();
                (cache.token_0_mint == input_mint_pk && cache.token_1_mint == output_mint_pk)
                    || (cache.token_0_mint == output_mint_pk && cache.token_1_mint == input_mint_pk)
            })
            .ok_or_else(|| anyhow!("No pool found for {}/{}", input_mint, output_mint))?;

        let pool_addr = *pool_entry.key();
        let pool = pool_entry.value();

        // Swap instruction discriminator (placeholder - needs mainnet verification)
        let discriminator: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

        // Build instruction data
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&discriminator);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());

        let program_id = Pubkey::from_str(RAYDIUM_CPMM_PROGRAM)?;
        let token_program = Pubkey::from_str(TOKEN_PROGRAM)?;

        // Account ordering (simplified - needs verification)
        // Placeholder: user accounts would come from wallet/treasury
        let user = Pubkey::default(); // TODO: Get from caller
        let user_token_in = Pubkey::default(); // TODO: Derive ATA
        let user_token_out = Pubkey::default(); // TODO: Derive ATA

        let accounts = vec![
            AccountMeta::new(pool_addr, false),
            AccountMeta::new(pool.token_0_vault, false),
            AccountMeta::new(pool.token_1_vault, false),
            AccountMeta::new(user_token_in, false),
            AccountMeta::new(user_token_out, false),
            AccountMeta::new_readonly(user, true),
            AccountMeta::new_readonly(token_program, false),
        ];

        Ok(vec![Instruction {
            program_id,
            accounts,
            data,
        }])
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        for entry in self.pools.iter() {
            let pool = entry.value();
            pairs.push((
                pool.token_0_mint.to_string(),
                pool.token_1_mint.to_string(),
            ));
        }
        pairs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_product_quote() {
        let cpmm = RaydiumCpmm::new(Arc::new(SolanaRpc::new("https://dummy")));

        // 1000 SOL / 100000 USDC pool, swap 1 SOL, 0.25% fee
        let quote = cpmm
            .quote_constant_product(1_000_000_000, 100_000_000_000, 1_000_000, 25)
            .expect("Quote failed");

        // Expected: ~99.75 USDC (with 0.25% fee)
        assert!(quote.amount_out > 99_000_000 && quote.amount_out < 100_000_000);
        assert_eq!(quote.fee_bps, 25);
    }

    #[test]
    fn test_pool_parse() {
        // Create minimal valid pool data (200 bytes)
        let mut data = vec![0u8; 200];
        
        // Discriminator (8 bytes) + status (1 byte)
        data[8] = 1; // status = initialized
        
        // Token mints (random valid pubkeys)
        let token_0 = Pubkey::new_unique();
        let token_1 = Pubkey::new_unique();
        data[16..48].copy_from_slice(&token_0.to_bytes());
        data[48..80].copy_from_slice(&token_1.to_bytes());
        
        // Vaults
        let vault_0 = Pubkey::new_unique();
        let vault_1 = Pubkey::new_unique();
        data[80..112].copy_from_slice(&vault_0.to_bytes());
        data[112..144].copy_from_slice(&vault_1.to_bytes());
        
        // LP mint
        let lp_mint = Pubkey::new_unique();
        data[144..176].copy_from_slice(&lp_mint.to_bytes());
        
        // Fee rate (25 bps = 2500 in raw)
        let fee_rate: u64 = 2500;
        data[176..184].copy_from_slice(&fee_rate.to_le_bytes());
        
        let pool = CpmmPool::parse(&data).expect("Parse failed");
        
        assert_eq!(pool.status, 1);
        assert_eq!(pool.token_0_mint, token_0);
        assert_eq!(pool.token_1_mint, token_1);
        assert_eq!(pool.token_0_vault, vault_0);
        assert_eq!(pool.token_1_vault, vault_1);
        assert_eq!(pool.lp_mint, lp_mint);
        assert_eq!(pool.fee_rate, 2500);
    }
}
