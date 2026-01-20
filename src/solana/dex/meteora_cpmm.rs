//! Meteora CPMM (DAMM V2) DEX Connector
//!
//! Implements the Dex trait for Meteora CPMM pools (Constant Product AMM).
//! This is simpler than DLMM - uses standard x*y=k formula without bins.
//!
//! Program ID: cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D
//!
//! ## Data Flow (NO RPC in hot path!)
//!
//! 1. **market-data** subscribes to CPMM accounts via Geyser
//! 2. **market-data** parses pools and emits `PoolCreated`/`PoolUpdated` events via NATS
//! 3. **Consumers** (momentum-bot, arb-strategy) receive events and update their local cache
//! 4. **execution-engine** receives pool data via `Intent.resources.accounts` (from DexPoolAccounts)
//! 5. **RPC is ONLY used as fallback** for liquidation when cache is empty

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info};

use super::meteora_cpmm_layout::{CpmmPool, CPMM_POOL_SIZE, METEORA_CPMM_PROGRAM};
use super::{Dex, Quote};

/// Default fee in basis points for Meteora CPMM (0.25%)
const DEFAULT_FEE_BPS: u64 = 25;

/// Cached pool state with reserve balances
#[derive(Clone, Debug)]
pub struct PoolCache {
    pub address: Pubkey,
    pub pool: CpmmPool,
    /// Reserve balance of token_0 vault
    pub reserve_0: u64,
    /// Reserve balance of token_1 vault
    pub reserve_1: u64,
    pub last_updated: std::time::SystemTime,
}

/// Meteora CPMM DEX Connector
///
/// **Data Source**: All pool data comes from Geyser via NATS events.
/// NO RPC calls in the hot path. RPC is only used as fallback for liquidation.
pub struct MeteoraCpmm {
    pools: Arc<DashMap<Pubkey, PoolCache>>,
    /// mint -> pool addresses index
    mint_index: Arc<DashMap<Pubkey, Vec<Pubkey>>>,
    /// User authority for building swap instructions
    user_authority: parking_lot::RwLock<Option<Pubkey>>,
}

impl MeteoraCpmm {
    /// Create a new CPMM connector (no RPC dependency - data comes from Geyser)
    pub fn new() -> Self {
        Self {
            pools: Arc::new(DashMap::new()),
            mint_index: Arc::new(DashMap::new()),
            user_authority: parking_lot::RwLock::new(None),
        }
    }

    /// Set user authority (wallet pubkey) for swap instructions
    pub fn set_user_authority(&mut self, auth: Pubkey) {
        *self.user_authority.write() = Some(auth);
    }

    /// Get a cached pool by address
    pub fn get_pool(&self, address: &Pubkey) -> Option<CpmmPool> {
        self.pools.get(address).map(|entry| entry.pool.clone())
    }

    /// Get full pool cache entry (including reserves)
    pub fn get_pool_cache(&self, address: &Pubkey) -> Option<PoolCache> {
        self.pools.get(address).map(|entry| entry.clone())
    }

    /// List all cached pool addresses
    pub fn list_pools(&self) -> Vec<Pubkey> {
        self.pools.iter().map(|entry| *entry.key()).collect()
    }

    /// Number of cached pools
    pub fn pool_count(&self) -> usize {
        self.pools.len()
    }

    /// Find pools containing a specific mint
    pub fn pools_for_mint(&self, mint: &Pubkey) -> Vec<Pubkey> {
        self.mint_index
            .get(mint)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    /// Parse pool from raw account data (used by market-data Geyser handler)
    ///
    /// This is a static helper for parsing Geyser account updates.
    /// Does NOT add to cache - caller should use `inject_pool_state` after parsing.
    pub fn parse_pool_data(data: &[u8]) -> Result<CpmmPool> {
        if data.len() < CPMM_POOL_SIZE {
            return Err(anyhow!(
                "CPMM pool data too short: {} < {}",
                data.len(),
                CPMM_POOL_SIZE
            ));
        }
        CpmmPool::parse(data)
    }

    /// Check if account data looks like a valid CPMM pool
    pub fn is_valid_pool_data(data: &[u8]) -> bool {
        if data.len() < CPMM_POOL_SIZE {
            return false;
        }
        // Check discriminator - Meteora CPMM pools start with specific bytes
        // The discriminator is the first 8 bytes
        // For now, just check size - could add discriminator check if needed
        true
    }

    /// Get pool accounts in DexPoolAccounts format for a given pool address.
    ///
    /// Returns accounts in the format expected by `set_pool_from_accounts`:
    /// - accounts[0] = pool_address
    /// - accounts[1] = token_0_mint
    /// - accounts[2] = token_1_mint
    /// - accounts[3] = token_0_vault
    /// - accounts[4] = token_1_vault
    /// - accounts[5] = amm_config
    /// - accounts[6] = observation_key
    /// - accounts[7] = "reserve_0:<value>"
    /// - accounts[8] = "reserve_1:<value>"
    ///
    /// Returns None if pool is not cached.
    pub fn get_pool_accounts(&self, pool_address: &Pubkey) -> Option<Vec<String>> {
        self.pools.get(pool_address).map(|entry| {
            let pool = &entry.pool;
            vec![
                pool_address.to_string(),
                pool.token_0_mint.to_string(),
                pool.token_1_mint.to_string(),
                pool.token_0_vault.to_string(),
                pool.token_1_vault.to_string(),
                pool.amm_config.to_string(),
                pool.observation_key.to_string(),
                format!("reserve_0:{}", entry.reserve_0),
                format!("reserve_1:{}", entry.reserve_1),
            ]
        })
    }

    /// Inject pool state from Geyser data
    ///
    /// This is the PRIMARY method to populate the cache. Called by:
    /// - market-data when it receives Geyser account updates
    /// - Consumers when they receive PoolCreated/PoolUpdated events via NATS
    pub fn inject_pool_state(
        &self,
        pool_address: Pubkey,
        pool: CpmmPool,
        reserve_0: u64,
        reserve_1: u64,
    ) -> bool {
        let is_new = !self.pools.contains_key(&pool_address);

        // Update mint index
        if is_new {
            self.mint_index
                .entry(pool.token_0_mint)
                .or_default()
                .push(pool_address);
            self.mint_index
                .entry(pool.token_1_mint)
                .or_default()
                .push(pool_address);
            
            debug!(
                pool = %pool_address,
                token_0 = %pool.token_0_mint,
                token_1 = %pool.token_1_mint,
                "meteora_cpmm: new pool added to cache"
            );
        }

        self.pools.insert(
            pool_address,
            PoolCache {
                address: pool_address,
                pool,
                reserve_0,
                reserve_1,
                last_updated: std::time::SystemTime::now(),
            },
        );

        is_new
    }

    /// Update only the reserves for an existing pool (from Geyser vault updates)
    pub fn update_reserves(&self, pool_address: &Pubkey, reserve_0: u64, reserve_1: u64) -> bool {
        if let Some(mut entry) = self.pools.get_mut(pool_address) {
            entry.reserve_0 = reserve_0;
            entry.reserve_1 = reserve_1;
            entry.last_updated = std::time::SystemTime::now();
            true
        } else {
            false
        }
    }

    /// Calculate output amount using constant product formula
    ///
    /// out = (reserve_out * amount_in * (10000 - fee_bps)) / (reserve_in * 10000 + amount_in * (10000 - fee_bps))
    fn calculate_swap_output(
        &self,
        amount_in: u64,
        reserve_in: u64,
        reserve_out: u64,
        fee_bps: u64,
    ) -> u64 {
        if reserve_in == 0 || reserve_out == 0 || amount_in == 0 {
            return 0;
        }

        // Use u128 to prevent overflow
        let amount_in = amount_in as u128;
        let reserve_in = reserve_in as u128;
        let reserve_out = reserve_out as u128;
        let fee_multiplier = (10000 - fee_bps) as u128;

        let numerator = reserve_out * amount_in * fee_multiplier;
        let denominator = reserve_in * 10000 + amount_in * fee_multiplier;

        if denominator == 0 {
            return 0;
        }

        (numerator / denominator) as u64
    }

    /// Derive the pool authority PDA
    fn derive_pool_authority(pool_address: &Pubkey) -> (Pubkey, u8) {
        let program_id = Pubkey::from_str(METEORA_CPMM_PROGRAM).unwrap();
        Pubkey::find_program_address(&[b"vault_and_lp_mint_auth_seed", pool_address.as_ref()], &program_id)
    }

    /// Build swap instruction for Meteora CPMM
    fn build_swap_instruction(
        &self,
        pool: &CpmmPool,
        pool_address: &Pubkey,
        user: &Pubkey,
        user_source_token: &Pubkey,
        user_destination_token: &Pubkey,
        amount_in: u64,
        minimum_amount_out: u64,
        is_base_input: bool, // true if swapping token_0 -> token_1
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(METEORA_CPMM_PROGRAM)?;
        let (authority, _bump) = Self::derive_pool_authority(pool_address);

        // Determine vault order based on swap direction
        let (input_vault, output_vault, input_mint, output_mint, input_program, output_program) =
            if is_base_input {
                (
                    pool.token_0_vault,
                    pool.token_1_vault,
                    pool.token_0_mint,
                    pool.token_1_mint,
                    pool.token_0_program,
                    pool.token_1_program,
                )
            } else {
                (
                    pool.token_1_vault,
                    pool.token_0_vault,
                    pool.token_1_mint,
                    pool.token_0_mint,
                    pool.token_1_program,
                    pool.token_0_program,
                )
            };

        // Instruction data: discriminator (8 bytes) + amount_in (8 bytes) + minimum_amount_out (8 bytes)
        // Swap discriminator for Meteora CPMM: [43, 4, 237, 11, 26, 201, 30, 98]
        let mut data = vec![43, 4, 237, 11, 26, 201, 30, 98];
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&minimum_amount_out.to_le_bytes());

        // Account order for swap_base_input / swap_base_output:
        // 0. payer (signer)
        // 1. authority (PDA)
        // 2. amm_config
        // 3. pool_state
        // 4. input_token_account (user's source)
        // 5. output_token_account (user's destination)
        // 6. input_vault
        // 7. output_vault
        // 8. input_token_program
        // 9. output_token_program
        // 10. input_token_mint
        // 11. output_token_mint
        // 12. observation_state

        let accounts = vec![
            AccountMeta::new(*user, true),                        // payer (signer)
            AccountMeta::new_readonly(authority, false),          // authority
            AccountMeta::new_readonly(pool.amm_config, false),    // amm_config
            AccountMeta::new(*pool_address, false),               // pool_state
            AccountMeta::new(*user_source_token, false),          // input_token_account
            AccountMeta::new(*user_destination_token, false),     // output_token_account
            AccountMeta::new(input_vault, false),                 // input_vault
            AccountMeta::new(output_vault, false),                // output_vault
            AccountMeta::new_readonly(input_program, false),      // input_token_program
            AccountMeta::new_readonly(output_program, false),     // output_token_program
            AccountMeta::new_readonly(input_mint, false),         // input_token_mint
            AccountMeta::new_readonly(output_mint, false),        // output_token_mint
            AccountMeta::new(pool.observation_key, false),        // observation_state
        ];

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }
}

#[async_trait]
impl Dex for MeteoraCpmm {
    async fn refresh_pools(&self) -> Result<()> {
        // For CPMM, we rely on Geyser injection rather than RPC scanning
        // This is called during initialization but actual pool data comes from Geyser
        info!("meteora_cpmm: refresh_pools called (pools injected via Geyser)");
        Ok(())
    }

    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>> {
        let input = Pubkey::from_str(input_mint)?;
        let output = Pubkey::from_str(output_mint)?;

        // Find pools containing the input mint
        let candidate_pools = self.pools_for_mint(&input);

        let mut best_quote: Option<Quote> = None;

        for pool_addr in candidate_pools {
            let Some(cache) = self.pools.get(&pool_addr) else {
                continue;
            };

            let pool = &cache.pool;

            // Check if pool contains both mints
            if !pool.contains_mint(&output) {
                continue;
            }

            // Check pool is active
            if !pool.is_active() {
                continue;
            }

            // Determine swap direction and reserves
            let (reserve_in, reserve_out, is_base_input) = if pool.token_0_mint == input {
                (cache.reserve_0, cache.reserve_1, true)
            } else {
                (cache.reserve_1, cache.reserve_0, false)
            };

            // Skip if no liquidity
            if reserve_in == 0 || reserve_out == 0 {
                continue;
            }

            // Calculate output
            let amount_out = self.calculate_swap_output(amount_in, reserve_in, reserve_out, DEFAULT_FEE_BPS);

            if amount_out == 0 {
                continue;
            }

            // Check if this is better than current best
            let is_better = match &best_quote {
                None => true,
                Some(q) => amount_out > q.amount_out,
            };

            if is_better {
                best_quote = Some(Quote {
                    input_mint: input_mint.to_string(),
                    output_mint: output_mint.to_string(),
                    amount_out,
                    route: vec![pool_addr.to_string()],
                    price_impact_bps: 0,
                    fee_bps: DEFAULT_FEE_BPS as u32,
                    in_reserve: reserve_in as u128,
                    out_reserve: reserve_out as u128,
                    tick_spacing: None,
                });

                debug!(
                    pool = %pool_addr,
                    amount_in,
                    amount_out,
                    reserve_in,
                    reserve_out,
                    is_base_input,
                    "meteora_cpmm: quote found"
                );
            }
        }

        Ok(best_quote)
    }

    fn build_swap_ix(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>> {
        let input = Pubkey::from_str(input_mint)?;
        let output = Pubkey::from_str(output_mint)?;

        let user = self
            .user_authority
            .read()
            .ok_or_else(|| anyhow!("User authority not set for meteora_cpmm"))?;

        // Find the best pool
        let candidate_pools = self.pools_for_mint(&input);

        for pool_addr in candidate_pools {
            let Some(cache) = self.pools.get(&pool_addr) else {
                continue;
            };

            let pool = &cache.pool;

            if !pool.contains_mint(&output) || !pool.is_active() {
                continue;
            }

            let is_base_input = pool.token_0_mint == input;

            // Determine input/output token programs based on swap direction
            let (input_program, output_program) = if is_base_input {
                (pool.token_0_program, pool.token_1_program)
            } else {
                (pool.token_1_program, pool.token_0_program)
            };

            // Convert to spl_token pubkeys for ATA derivation
            let user_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(user.to_bytes());
            let input_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(input.to_bytes());
            let output_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(output.to_bytes());
            let input_program_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(input_program.to_bytes());
            let output_program_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(output_program.to_bytes());

            // Derive user token accounts (ATAs) with correct token programs
            let user_source_token_spl =
                spl_associated_token_account::get_associated_token_address_with_program_id(
                    &user_spl,
                    &input_spl,
                    &input_program_spl,
                );
            let user_destination_token_spl =
                spl_associated_token_account::get_associated_token_address_with_program_id(
                    &user_spl,
                    &output_spl,
                    &output_program_spl,
                );

            // Convert back to solana_sdk Pubkey
            let user_source_token = Pubkey::new_from_array(user_source_token_spl.to_bytes());
            let user_destination_token = Pubkey::new_from_array(user_destination_token_spl.to_bytes());

            let ix = self.build_swap_instruction(
                pool,
                &pool_addr,
                &user,
                &user_source_token,
                &user_destination_token,
                amount_in,
                min_out,
                is_base_input,
            )?;

            info!(
                pool = %pool_addr,
                input_mint,
                output_mint,
                amount_in,
                min_out,
                is_base_input,
                "meteora_cpmm: built swap instruction"
            );

            return Ok(vec![ix]);
        }

        Err(anyhow!(
            "meteora_cpmm: no suitable pool found for {} -> {}",
            input_mint,
            output_mint
        ))
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        self.pools
            .iter()
            .map(|entry| {
                let pool = &entry.pool;
                (pool.token_0_mint.to_string(), pool.token_1_mint.to_string())
            })
            .collect()
    }

    /// Set pool from DexPoolAccounts format (for Intent processing)
    ///
    /// Expected accounts format:
    /// - accounts[0] = pool_address
    /// - accounts[1] = token_0_mint
    /// - accounts[2] = token_1_mint
    /// - accounts[3] = token_0_vault
    /// - accounts[4] = token_1_vault
    /// - accounts[5] = amm_config
    /// - accounts[6] = observation_key
    /// - accounts[7+] = tagged values: "reserve_0:<value>", "reserve_1:<value>"
    fn set_pool_from_accounts(&self, pool_address: &str, accounts: &[String]) -> Result<()> {
        if accounts.len() < 7 {
            return Err(anyhow!(
                "meteora_cpmm set_pool_from_accounts requires at least 7 accounts, got {}",
                accounts.len()
            ));
        }

        let parse_pubkey = |s: &str, name: &str| -> Result<Pubkey> {
            Pubkey::from_str(s).map_err(|e| anyhow!("Invalid {} pubkey '{}': {}", name, s, e))
        };

        let pool_pk = parse_pubkey(pool_address, "pool_address")?;

        // Validate accounts[0] matches pool_address
        let expected_pool = parse_pubkey(&accounts[0], "accounts[0]")?;
        if pool_pk != expected_pool {
            return Err(anyhow!(
                "pool_address {} does not match accounts[0] {}",
                pool_address,
                expected_pool
            ));
        }

        let token_0_mint = parse_pubkey(&accounts[1], "token_0_mint")?;
        let token_1_mint = parse_pubkey(&accounts[2], "token_1_mint")?;
        let token_0_vault = parse_pubkey(&accounts[3], "token_0_vault")?;
        let token_1_vault = parse_pubkey(&accounts[4], "token_1_vault")?;
        let amm_config = parse_pubkey(&accounts[5], "amm_config")?;
        let observation_key = parse_pubkey(&accounts[6], "observation_key")?;

        // Parse tagged values for reserves
        let mut reserve_0: u64 = 0;
        let mut reserve_1: u64 = 0;

        for account in accounts.iter().skip(7) {
            if let Some(value) = account.strip_prefix("reserve_0:") {
                if let Ok(v) = value.parse::<u64>() {
                    reserve_0 = v;
                }
            } else if let Some(value) = account.strip_prefix("reserve_1:") {
                if let Ok(v) = value.parse::<u64>() {
                    reserve_1 = v;
                }
            }
        }

        // Create minimal pool structure
        let pool = CpmmPool {
            discriminator: [0u8; 8],
            amm_config,
            pool_creator: Pubkey::default(),
            token_0_vault,
            token_1_vault,
            lp_mint: Pubkey::default(),
            token_0_mint,
            token_1_mint,
            token_0_program: Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap(),
            token_1_program: Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap(),
            observation_key,
            auth_bump: 0,
            status: 0,
            lp_mint_decimals: 0,
            mint_0_decimals: 0,
            mint_1_decimals: 0,
            lp_supply: 0,
            protocol_fees_token_0: 0,
            protocol_fees_token_1: 0,
            fund_fees_token_0: 0,
            fund_fees_token_1: 0,
            open_time: 0,
        };

        self.inject_pool_state(pool_pk, pool, reserve_0, reserve_1);

        debug!(
            pool = %pool_pk,
            token_0_mint = %token_0_mint,
            token_1_mint = %token_1_mint,
            reserve_0,
            reserve_1,
            "meteora_cpmm: set_pool_from_accounts"
        );

        Ok(())
    }
}

impl Default for MeteoraCpmm {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_product_calculation() {
        let cpmm = MeteoraCpmm::new();

        // Test with typical values
        // 1000 token in, 1000000 reserve_in, 1000000 reserve_out, 25 bps fee
        let out = cpmm.calculate_swap_output(1000, 1_000_000, 1_000_000, 25);
        // Expected: ~997 (slightly less due to fee and price impact)
        assert!(out > 990 && out < 1000, "Output was {}", out);

        // Test with zero reserves
        assert_eq!(cpmm.calculate_swap_output(1000, 0, 1_000_000, 25), 0);
        assert_eq!(cpmm.calculate_swap_output(1000, 1_000_000, 0, 25), 0);

        // Test with zero input
        assert_eq!(cpmm.calculate_swap_output(0, 1_000_000, 1_000_000, 25), 0);
    }

    #[test]
    fn test_inject_and_query_pool() {
        let cpmm = MeteoraCpmm::new();
        
        let pool_addr = Pubkey::new_unique();
        let token_0 = Pubkey::new_unique();
        let token_1 = Pubkey::new_unique();
        
        let pool = CpmmPool {
            discriminator: [0u8; 8],
            amm_config: Pubkey::new_unique(),
            pool_creator: Pubkey::new_unique(),
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            lp_mint: Pubkey::new_unique(),
            token_0_mint: token_0,
            token_1_mint: token_1,
            token_0_program: Pubkey::new_unique(),
            token_1_program: Pubkey::new_unique(),
            observation_key: Pubkey::new_unique(),
            auth_bump: 255,
            status: 0,
            lp_mint_decimals: 9,
            mint_0_decimals: 9,
            mint_1_decimals: 6,
            lp_supply: 1_000_000,
            protocol_fees_token_0: 0,
            protocol_fees_token_1: 0,
            fund_fees_token_0: 0,
            fund_fees_token_1: 0,
            open_time: 0,
        };
        
        // Inject pool
        let is_new = cpmm.inject_pool_state(pool_addr, pool.clone(), 100_000_000, 50_000_000);
        assert!(is_new);
        
        // Query by address
        assert!(cpmm.get_pool(&pool_addr).is_some());
        assert_eq!(cpmm.pool_count(), 1);
        
        // Query by mint
        let pools_for_token_0 = cpmm.pools_for_mint(&token_0);
        assert_eq!(pools_for_token_0.len(), 1);
        assert_eq!(pools_for_token_0[0], pool_addr);
        
        // Second inject is not new
        let is_new2 = cpmm.inject_pool_state(pool_addr, pool, 100_000_000, 50_000_000);
        assert!(!is_new2);
    }
}
