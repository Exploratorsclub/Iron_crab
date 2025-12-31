//! Cross-DEX Arbitrage Handler
//!
//! Handles arbitrage intents by:
//! 1. Fetching live quotes from both DEXes
//! 2. Validating the spread is still profitable
//! 3. Building swap instructions
//! 4. Creating atomic Jito bundle
//!
//! This module is used by execution-engine to process arb-strategy intents.

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::ipc::TradeIntent;
use crate::solana::dex::{pumpfun::PumpFunDex, raydium::Raydium, Dex, Quote};
use crate::solana::solana_rpc::SolanaRpc;

/// SOL mint address
pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Minimum spread in bps to execute after re-validation
const MIN_EXECUTION_SPREAD_BPS: i64 = 30; // 0.3% minimum after live quote

/// Result of Cross-DEX arbitrage validation
#[derive(Debug)]
pub struct CrossDexValidation {
    pub is_valid: bool,
    pub buy_quote: Option<Quote>,
    pub sell_quote: Option<Quote>,
    pub actual_spread_bps: i64,
    pub estimated_profit_lamports: i64,
    pub reject_reason: Option<String>,
}

/// Result of building Cross-DEX swap instructions
#[derive(Debug)]
pub struct CrossDexSwapPlan {
    pub buy_instructions: Vec<Instruction>,
    pub sell_instructions: Vec<Instruction>,
    pub total_compute_units: u32,
    pub buy_dex: String,
    pub sell_dex: String,
    pub input_sol_lamports: u64,
    pub expected_output_sol_lamports: u64,
}

/// Cross-DEX Arbitrage Handler
/// 
/// Manages DEX connectors and provides methods for validating and executing
/// cross-DEX arbitrage opportunities.
pub struct CrossDexHandler {
    rpc: Arc<SolanaRpc>,
    /// DEX connectors by name
    dexes: HashMap<String, Arc<dyn Dex>>,
    /// Wallet keypair (for building transactions)
    wallet: Option<Keypair>,
    /// Default slippage for swaps (bps)
    default_slippage_bps: u32,
}

impl CrossDexHandler {
    /// Create a new CrossDexHandler
    pub fn new(rpc: Arc<SolanaRpc>, wallet: Option<Keypair>) -> Self {
        Self {
            rpc,
            dexes: HashMap::new(),
            wallet,
            default_slippage_bps: 100, // 1% default
        }
    }

    /// Initialize DEX connectors
    pub async fn init_dexes(&mut self) -> Result<()> {
        // Initialize Raydium
        let raydium = Raydium::new(Arc::clone(&self.rpc));
        self.dexes.insert("raydium".to_string(), Arc::new(raydium));
        info!("Initialized Raydium DEX connector");

        // Initialize PumpFun
        let pumpfun = PumpFunDex::new(Arc::clone(&self.rpc))?;
        self.dexes.insert("pumpfun".to_string(), Arc::new(pumpfun));
        info!("Initialized PumpFun DEX connector");

        // TODO: Add Orca Whirlpool
        // let orca = OrcaDex::new((*self.rpc).clone());
        // self.dexes.insert("orca".to_string(), Arc::new(orca));

        Ok(())
    }

    /// Check if this intent is a cross-DEX arbitrage intent
    pub fn is_cross_dex_arb_intent(intent: &TradeIntent) -> bool {
        // Cross-DEX arb intents have 2 pools and require_bundle
        intent.resources.pools.len() == 2 
            && intent.requires_bundle()
            && intent.source == "arb-strategy"
    }

    /// Validate a cross-DEX arbitrage opportunity with live quotes
    /// 
    /// This fetches fresh quotes from both DEXes and validates:
    /// 1. The spread is still profitable
    /// 2. Both DEXes have sufficient liquidity
    /// 3. The estimated profit covers transaction costs
    pub async fn validate_arb_opportunity(
        &self,
        intent: &TradeIntent,
        tx_cost_lamports: u64,
    ) -> Result<CrossDexValidation> {
        // Parse pools from intent
        // Expected format in metadata or pool addresses
        let pools = &intent.resources.pools;
        if pools.len() != 2 {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: None,
                sell_quote: None,
                actual_spread_bps: 0,
                estimated_profit_lamports: 0,
                reject_reason: Some("Expected exactly 2 pools for cross-DEX arb".to_string()),
            });
        }

        let token_mint = &intent.resources.output_mint;
        let trade_amount = intent.required_capital.raw;

        // Determine which DEX each pool belongs to
        let (buy_dex, buy_pool) = self.identify_dex(&pools[0])?;
        let (sell_dex, sell_pool) = self.identify_dex(&pools[1])?;

        debug!(
            token = %token_mint,
            buy_dex = %buy_dex,
            sell_dex = %sell_dex,
            amount = trade_amount,
            "Validating cross-DEX arb opportunity"
        );

        // Get live quote from buy DEX (SOL -> Token)
        let buy_connector = self.dexes.get(&buy_dex)
            .ok_or_else(|| anyhow!("Unknown buy DEX: {}", buy_dex))?;
        
        let buy_quote = buy_connector
            .quote_exact_in(SOL_MINT, token_mint, trade_amount)
            .await?;

        let buy_quote = match buy_quote {
            Some(q) => q,
            None => {
                return Ok(CrossDexValidation {
                    is_valid: false,
                    buy_quote: None,
                    sell_quote: None,
                    actual_spread_bps: 0,
                    estimated_profit_lamports: 0,
                    reject_reason: Some(format!("No buy quote available from {}", buy_dex)),
                });
            }
        };

        // Get live quote from sell DEX (Token -> SOL)
        let sell_connector = self.dexes.get(&sell_dex)
            .ok_or_else(|| anyhow!("Unknown sell DEX: {}", sell_dex))?;

        let sell_quote = sell_connector
            .quote_exact_in(token_mint, SOL_MINT, buy_quote.amount_out)
            .await?;

        let sell_quote = match sell_quote {
            Some(q) => q,
            None => {
                return Ok(CrossDexValidation {
                    is_valid: false,
                    buy_quote: Some(buy_quote),
                    sell_quote: None,
                    actual_spread_bps: 0,
                    estimated_profit_lamports: 0,
                    reject_reason: Some(format!("No sell quote available from {}", sell_dex)),
                });
            }
        };

        // Calculate actual spread
        // We input X SOL, buy tokens, sell tokens, get Y SOL
        // Spread = (Y - X) / X * 10000 bps
        let input_sol = trade_amount as i64;
        let output_sol = sell_quote.amount_out as i64;
        let gross_profit = output_sol - input_sol;
        let actual_spread_bps = if input_sol > 0 {
            (gross_profit * 10000) / input_sol
        } else {
            0
        };

        // Subtract tx costs
        let net_profit = gross_profit - tx_cost_lamports as i64;

        info!(
            input_sol = input_sol,
            output_sol = output_sol,
            gross_profit = gross_profit,
            tx_cost = tx_cost_lamports,
            net_profit = net_profit,
            spread_bps = actual_spread_bps,
            "Cross-DEX arb validation result"
        );

        // Validate profitability
        if actual_spread_bps < MIN_EXECUTION_SPREAD_BPS {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: Some(buy_quote),
                sell_quote: Some(sell_quote),
                actual_spread_bps,
                estimated_profit_lamports: net_profit,
                reject_reason: Some(format!(
                    "Spread too low: {}bps < {}bps minimum",
                    actual_spread_bps, MIN_EXECUTION_SPREAD_BPS
                )),
            });
        }

        if net_profit <= 0 {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: Some(buy_quote),
                sell_quote: Some(sell_quote),
                actual_spread_bps,
                estimated_profit_lamports: net_profit,
                reject_reason: Some(format!(
                    "Not profitable after tx costs: {} lamports",
                    net_profit
                )),
            });
        }

        Ok(CrossDexValidation {
            is_valid: true,
            buy_quote: Some(buy_quote),
            sell_quote: Some(sell_quote),
            actual_spread_bps,
            estimated_profit_lamports: net_profit,
            reject_reason: None,
        })
    }

    /// Build swap instructions for cross-DEX arbitrage
    ///
    /// Creates instructions for:
    /// 1. Buy leg: SOL -> Token on buy_dex
    /// 2. Sell leg: Token -> SOL on sell_dex
    pub async fn build_swap_plan(
        &self,
        intent: &TradeIntent,
        validation: &CrossDexValidation,
    ) -> Result<CrossDexSwapPlan> {
        let buy_quote = validation.buy_quote.as_ref()
            .ok_or_else(|| anyhow!("No buy quote for swap plan"))?;
        let sell_quote = validation.sell_quote.as_ref()
            .ok_or_else(|| anyhow!("No sell quote for swap plan"))?;

        let pools = &intent.resources.pools;
        let token_mint = &intent.resources.output_mint;
        let trade_amount = intent.required_capital.raw;

        let (buy_dex, _) = self.identify_dex(&pools[0])?;
        let (sell_dex, _) = self.identify_dex(&pools[1])?;

        // Calculate min_out with slippage
        let slippage_bps = intent.max_slippage_bps.min(self.default_slippage_bps);
        
        // Buy leg: min tokens out
        let buy_min_out = buy_quote.amount_out
            .saturating_mul(10000 - slippage_bps as u64)
            / 10000;

        // Sell leg: min SOL out
        let sell_min_out = sell_quote.amount_out
            .saturating_mul(10000 - slippage_bps as u64)
            / 10000;

        // Build buy instructions
        let buy_connector = self.dexes.get(&buy_dex)
            .ok_or_else(|| anyhow!("Unknown buy DEX: {}", buy_dex))?;

        let buy_instructions = buy_connector.build_swap_ix(
            SOL_MINT,
            token_mint,
            trade_amount,
            buy_min_out,
        )?;

        // Build sell instructions
        let sell_connector = self.dexes.get(&sell_dex)
            .ok_or_else(|| anyhow!("Unknown sell DEX: {}", sell_dex))?;

        let sell_instructions = sell_connector.build_swap_ix(
            token_mint,
            SOL_MINT,
            buy_quote.amount_out, // Use expected tokens from buy
            sell_min_out,
        )?;

        // Estimate compute units
        let total_cu = 200_000 * 2; // ~200k per swap

        Ok(CrossDexSwapPlan {
            buy_instructions,
            sell_instructions,
            total_compute_units: total_cu,
            buy_dex,
            sell_dex,
            input_sol_lamports: trade_amount,
            expected_output_sol_lamports: sell_quote.amount_out,
        })
    }

    /// Build a Jito-compatible transaction bundle for atomic execution
    pub fn build_atomic_bundle(
        &self,
        plan: &CrossDexSwapPlan,
        recent_blockhash: solana_sdk::hash::Hash,
        priority_fee_lamports: u64,
    ) -> Result<Transaction> {
        let wallet = self.wallet.as_ref()
            .ok_or_else(|| anyhow!("No wallet configured for transaction building"))?;

        let mut all_instructions = Vec::new();

        // Add compute budget instructions
        all_instructions.push(
            solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(
                plan.total_compute_units,
            )
        );
        
        if priority_fee_lamports > 0 {
            // Convert lamports to micro-lamports per CU
            let micro_lamports_per_cu = (priority_fee_lamports * 1_000_000) / plan.total_compute_units as u64;
            all_instructions.push(
                solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(
                    micro_lamports_per_cu,
                )
            );
        }

        // Add buy leg
        all_instructions.extend(plan.buy_instructions.clone());

        // Add sell leg
        all_instructions.extend(plan.sell_instructions.clone());

        // Build transaction
        let tx = Transaction::new_signed_with_payer(
            &all_instructions,
            Some(&wallet.pubkey()),
            &[wallet],
            recent_blockhash,
        );

        Ok(tx)
    }

    /// Identify which DEX a pool belongs to based on address pattern
    fn identify_dex(&self, pool_address: &str) -> Result<(String, String)> {
        // Known program IDs
        const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
        const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
        const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";

        // For now, use simple heuristics based on pool address length and pattern
        // In production: query on-chain owner or use cached pool registry
        
        // Raydium AMM pools typically have specific patterns
        // PumpFun bonding curves are derived from mint
        // Orca whirlpools have their own pattern

        // Simple heuristic: check if pool exists in our connectors
        // This is a placeholder - real implementation would query chain
        
        let pubkey = Pubkey::from_str(pool_address)
            .map_err(|_| anyhow!("Invalid pool address: {}", pool_address))?;

        // Try to identify by checking pool data (simplified)
        // In production: use account owner check
        
        // Default to checking if pool contains certain markers
        // This is a STUB - needs real implementation
        if pool_address.starts_with("5") || pool_address.starts_with("7") {
            // Likely Raydium
            Ok(("raydium".to_string(), pool_address.to_string()))
        } else if pool_address.len() == 44 {
            // Could be PumpFun bonding curve
            Ok(("pumpfun".to_string(), pool_address.to_string()))
        } else {
            // Default to Raydium
            warn!(pool = %pool_address, "Could not identify DEX, defaulting to Raydium");
            Ok(("raydium".to_string(), pool_address.to_string()))
        }
    }

    /// Get a reference to a DEX connector by name
    pub fn get_dex(&self, name: &str) -> Option<&Arc<dyn Dex>> {
        self.dexes.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_cross_dex_arb_intent() {
        // Would need mock TradeIntent for proper testing
    }
}
