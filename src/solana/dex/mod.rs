
use async_trait::async_trait;
use anyhow::Result;
use solana_sdk::instruction::Instruction;

pub mod raydium;
pub mod orca;
pub mod router;
pub mod orca_whirlpool_layout; // layout + heuristics

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

#[async_trait]
pub trait Dex: Send + Sync {
    async fn refresh_pools(&self) -> Result<()>;
    async fn quote_exact_in(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Result<Option<Quote>>;
    fn build_swap_ix(&self, input_mint: &str, output_mint: &str, amount_in: u64, min_out: u64) -> Result<Vec<Instruction>>;
    /// List available direct trading pairs (unordered; include both directions if symmetric desired).
    fn list_pairs(&self) -> Vec<(String, String)>;
}
