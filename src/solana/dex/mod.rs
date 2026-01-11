use anyhow::Result;
use async_trait::async_trait;
use solana_sdk::instruction::Instruction;

pub mod meteora_bin_array_layout;
pub mod meteora_bin_walker;
pub mod meteora_dlmm;
pub mod meteora_dlmm_layout;
pub mod meteora_swap_builder;
pub mod orca;
pub mod orca_reserve_cache;
pub mod orca_whirlpool_layout;
pub mod pumpfun;
pub mod pumpfun_amm;
pub mod raydium;
pub mod raydium_cpmm;
pub mod router; // layout + heuristics

#[derive(Debug, Clone)]
pub struct Quote {
    pub amount_out: u64,
    pub price_impact_bps: u32,
    pub route: Vec<String>, // z.B. Pool IDs
    pub fee_bps: u32,
    pub in_reserve: u128,
    pub out_reserve: u128,
    pub input_mint: String,
    pub output_mint: String,
    pub tick_spacing: Option<u16>,
}

use solana_sdk::pubkey::Pubkey;

#[async_trait]
pub trait Dex: Send + Sync {
    async fn refresh_pools(&self) -> Result<()>;
    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>>;
    fn build_swap_ix(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>>;
    /// List available direct trading pairs (unordered; include both directions if symmetric desired).
    fn list_pairs(&self) -> Vec<(String, String)>;
    
    /// Load a specific pool by address into the connector's cache.
    /// 
    /// This is used by execution-engine to load pools from Intent metadata
    /// without doing getProgramAccounts discovery. The pool address comes
    /// from arb-strategy which has Geyser data.
    /// 
    /// Default implementation returns Ok(()) (no-op for DEXes that don't need pre-loading).
    async fn load_pool_by_address(&self, _pool_address: &Pubkey) -> Result<()> {
        Ok(())
    }

    /// Async version of build_swap_ix for DEXes that need async operations
    /// (e.g., PumpFun bonding curve needs to fetch creator from chain).
    ///
    /// Default implementation calls the sync `build_swap_ix()`.
    /// Override for DEXes that require async (PumpFun, etc.).
    async fn build_swap_ix_async(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>> {
        // Default: call sync version
        self.build_swap_ix(input_mint, output_mint, amount_in, min_out)
    }
}
