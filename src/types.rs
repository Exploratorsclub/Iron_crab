use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

// Known DEX fee vault addresses for protocol fee attribution
pub mod fee_vaults {
    use super::*;
    
    // Raydium protocol fee vaults (well-known addresses)
    // Main Raydium AMM fee destination
    pub fn raydium_fee_owner() -> Pubkey {
        Pubkey::from_str("5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1").unwrap()
    }
    
    // Orca protocol fee vaults
    // Orca fee collector (common destination)
    pub fn orca_fee_owner() -> Pubkey {
        Pubkey::from_str("3xxgYc3jXPdjqpMdrRyKtcddh4ZdtqpaN33fwaWJ2Wbh").unwrap()
    }
    
    pub fn is_raydium_fee_vault(pubkey: &Pubkey) -> bool {
        *pubkey == raydium_fee_owner()
    }
    
    pub fn is_orca_fee_vault(pubkey: &Pubkey) -> bool {
        *pubkey == orca_fee_owner()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeeBreakdown {
    /// Total protocol fees (all DEXs combined, SOL micro lamports)
    pub protocol_fee_total_sol_micro: u64,
    /// Raydium-specific protocol fee
    pub raydium_protocol_fee_sol_micro: u64,
    /// Orca-specific protocol fee
    pub orca_protocol_fee_sol_micro: u64,
    /// Referrer fee (if any)
    pub referrer_fee_sol_micro: u64,
    /// Compute budget overhead (compute units * priority fee)
    pub compute_overhead_sol_micro: u64,
    /// Network base fee
    pub network_fee_lamports: u64,
}

impl FeeBreakdown {
    pub fn total_fees_sol_micro(&self) -> u64 {
        self.protocol_fee_total_sol_micro
            .saturating_add(self.referrer_fee_sol_micro)
            .saturating_add(self.compute_overhead_sol_micro)
            .saturating_add(self.network_fee_lamports.saturating_mul(1000))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Amount {
    pub ui: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub symbol: String,
    pub mint: String,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeIntent {
    pub market: String,
    pub base: Token,
    pub quote: Token,
    pub side: Side,
    pub amount: Amount,
    pub max_slippage_bps: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}
