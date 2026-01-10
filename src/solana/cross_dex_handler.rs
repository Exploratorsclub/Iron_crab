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
use solana_sdk::{instruction::Instruction, pubkey::Pubkey};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::ipc::TradeIntent;
use crate::solana::dex::{
    meteora_dlmm::MeteoraDlmm, pumpfun::PumpFunDex, pumpfun_amm::PumpFunAmmDex, raydium::Raydium,
    Dex, Quote,
};
use crate::solana::rpc::SolanaRpc;

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
    /// Wallet pubkey (used as user authority / payer)
    wallet_pubkey: Option<Pubkey>,
    /// Default slippage for swaps (bps)
    default_slippage_bps: u32,
    /// RPC URL for DEXes that need it directly
    rpc_url: Option<String>,
}

impl CrossDexHandler {
    async fn estimate_min_amount_in_for_target_out(
        &self,
        dex: &Arc<dyn Dex>,
        input_mint: &str,
        output_mint: &str,
        target_out: u64,
        max_in: u64,
        max_in_out: u64,
    ) -> Result<Option<u64>> {
        if target_out == 0 || max_in == 0 {
            return Ok(None);
        }

        if max_in_out < target_out {
            return Ok(None);
        }

        // Binary-search the smallest amount_in that yields >= target_out.
        // Assumes quote_exact_in is monotonic in amount_in for the current pool state.
        let mut lo: u64 = 1;
        let mut hi: u64 = max_in;
        for _ in 0..24 {
            if lo >= hi {
                break;
            }
            let mid = lo + (hi - lo) / 2;
            let q = dex.quote_exact_in(input_mint, output_mint, mid).await?;
            let out = q.map(|qq| qq.amount_out).unwrap_or(0);
            if out >= target_out {
                hi = mid;
            } else {
                lo = mid.saturating_add(1);
            }
        }

        Ok(Some(hi))
    }

    /// Create a new CrossDexHandler
    pub fn new(rpc: Arc<SolanaRpc>, wallet_pubkey: Option<Pubkey>) -> Self {
        Self {
            rpc,
            dexes: HashMap::new(),
            wallet_pubkey,
            default_slippage_bps: 100, // 1% default
            rpc_url: None,
        }
    }

    /// Set RPC URL for DEXes that need direct HTTP access
    pub fn with_rpc_url(mut self, url: String) -> Self {
        self.rpc_url = Some(url);
        self
    }

    /// Initialize DEX connectors
    pub async fn init_dexes(&mut self) -> Result<()> {
        // Initialize Raydium
        let mut raydium = Raydium::new(Arc::clone(&self.rpc));
        if let Some(pk) = self.wallet_pubkey {
            raydium.set_user_authority(pk);
        }
        self.dexes.insert("raydium".to_string(), Arc::new(raydium));
        info!("Initialized Raydium DEX connector");

        // Initialize PumpFun (Bonding Curve)
        let mut pumpfun = PumpFunDex::new(Arc::clone(&self.rpc))?;
        if let Some(pk) = self.wallet_pubkey {
            pumpfun.set_user_authority(pk);
        }
        self.dexes.insert("pumpfun".to_string(), Arc::new(pumpfun));
        info!("Initialized PumpFun DEX connector");

        // Initialize PumpSwap AMM (pump_amm)
        if let Some(ref rpc_url) = self.rpc_url {
            let pump_amm = PumpFunAmmDex::new(
                Arc::clone(&self.rpc),
                rpc_url.clone(),
                None, // No separate Helius URL for now
            );
            self.dexes
                .insert("pump_amm".to_string(), Arc::new(pump_amm));
            info!("Initialized PumpSwap AMM (pump_amm) DEX connector");
        } else {
            warn!("PumpSwap AMM not initialized: RPC URL not provided");
        }

        // Initialize Meteora DLMM
        let meteora = MeteoraDlmm::new(Arc::clone(&self.rpc));
        self.dexes
            .insert("meteora_dlmm".to_string(), Arc::new(meteora));
        info!("Initialized Meteora DLMM DEX connector");

        // TODO: Add Orca Whirlpool
        // let mut orca = Orca::new(Arc::clone(&self.rpc));
        // if let Some(pk) = self.wallet_pubkey {
        //     orca.set_user_authority(pk);
        // }
        // self.dexes.insert("orca".to_string(), Arc::new(orca));

        Ok(())
    }

    /// Check if this intent is a cross-DEX arbitrage intent
    pub fn is_cross_dex_arb_intent(intent: &TradeIntent) -> bool {
        // Cross-DEX arb intents have 2 pools.
        // Do NOT require metadata.dex; routing is driven by buy_dex/sell_dex metadata.
        intent.resources.pools.len() == 2 && intent.source == "arb-strategy"
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

        // Extract DEX info from intent metadata (set by arb-strategy)
        // Fallback to identify_dex if metadata not present
        let buy_dex = intent.metadata.get("buy_dex").cloned().unwrap_or_else(|| {
            self.identify_dex(&pools[0])
                .map(|(d, _)| d)
                .unwrap_or_default()
        });
        let sell_dex = intent.metadata.get("sell_dex").cloned().unwrap_or_else(|| {
            self.identify_dex(&pools[1])
                .map(|(d, _)| d)
                .unwrap_or_default()
        });

        // Log original spread from strategy for comparison
        if let Some(orig_spread) = intent.metadata.get("spread_bps") {
            debug!(
                original_spread_bps = %orig_spread,
                "Cross-DEX validation using strategy's original spread for reference"
            );
        }

        debug!(
            token = %token_mint,
            buy_dex = %buy_dex,
            sell_dex = %sell_dex,
            amount = trade_amount,
            "Validating cross-DEX arb opportunity"
        );

        // Get live quote from buy DEX (SOL -> Token)
        let buy_connector = self
            .dexes
            .get(&buy_dex)
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

        // Compute a conservative minimum-out for the buy leg, then quote the sell leg using
        // that guaranteed amount. This prevents building a 2nd-leg sell that requires more
        // tokens than we might actually receive (even if the buy meets its min_out).
        let slippage_bps = intent.max_slippage_bps.min(self.default_slippage_bps);
        let buy_min_out = buy_quote
            .amount_out
            .saturating_mul(10_000u64.saturating_sub(slippage_bps as u64))
            / 10_000u64;

        if buy_min_out == 0 {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: Some(buy_quote),
                sell_quote: None,
                actual_spread_bps: 0,
                estimated_profit_lamports: 0,
                reject_reason: Some("buy_min_out=0 (cannot build safe arb)".to_string()),
            });
        }

        // Estimate the minimum input needed to achieve buy_min_out.
        // This approximates "exact out" behavior: we try to buy a fixed token amount with a
        // max SOL spend (trade_amount) by choosing the smallest amount_in that still yields
        // >= buy_min_out. This minimizes leftover tokens when we sell exactly buy_min_out.
        let buy_amount_in_est = self
            .estimate_min_amount_in_for_target_out(
                buy_connector,
                SOL_MINT,
                token_mint,
                buy_min_out,
                trade_amount,
                buy_quote.amount_out,
            )
            .await?;

        let buy_amount_in_est = match buy_amount_in_est {
            Some(v) if v <= trade_amount => v,
            _ => {
                return Ok(CrossDexValidation {
                    is_valid: false,
                    buy_quote: Some(buy_quote),
                    sell_quote: None,
                    actual_spread_bps: 0,
                    estimated_profit_lamports: 0,
                    reject_reason: Some(
                        "cannot estimate amount_in for buy_min_out within max capital".to_string(),
                    ),
                });
            }
        };

        // Get live quote from sell DEX (Token -> SOL) based on conservative buy_min_out
        let sell_connector = self
            .dexes
            .get(&sell_dex)
            .ok_or_else(|| anyhow!("Unknown sell DEX: {}", sell_dex))?;

        let sell_quote = sell_connector
            .quote_exact_in(token_mint, SOL_MINT, buy_min_out)
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

        // Calculate spread based on estimated input for buy_min_out.
        // We input ~X SOL, buy >= buy_min_out tokens, sell buy_min_out tokens, get Y SOL.
        // Spread = (Y - X) / X * 10000 bps
        let input_sol = buy_amount_in_est as i64;
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
            max_input_sol = trade_amount,
            buy_amount_in_est = buy_amount_in_est,
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
        let buy_quote = validation
            .buy_quote
            .as_ref()
            .ok_or_else(|| anyhow!("No buy quote for swap plan"))?;
        let sell_quote = validation
            .sell_quote
            .as_ref()
            .ok_or_else(|| anyhow!("No sell quote for swap plan"))?;

        let pools = &intent.resources.pools;
        let token_mint = &intent.resources.output_mint;
        let trade_amount = intent.required_capital.raw;

        // Prefer explicit metadata set by arb-strategy.
        // The identify_dex() heuristic is a stub and must not be the primary routing source.
        let buy_dex = intent.metadata.get("buy_dex").cloned().unwrap_or_else(|| {
            self.identify_dex(&pools[0])
                .map(|(d, _)| d)
                .unwrap_or_default()
        });
        let sell_dex = intent.metadata.get("sell_dex").cloned().unwrap_or_else(|| {
            self.identify_dex(&pools[1])
                .map(|(d, _)| d)
                .unwrap_or_default()
        });

        if buy_dex.is_empty() || sell_dex.is_empty() {
            return Err(anyhow!(
                "cannot build swap plan: missing buy_dex/sell_dex routing hints"
            ));
        }

        // Calculate min_out with slippage
        let slippage_bps = intent.max_slippage_bps.min(self.default_slippage_bps);

        // Buy leg: min tokens out
        let buy_min_out = buy_quote
            .amount_out
            .saturating_mul(10000 - slippage_bps as u64)
            / 10000;

        if buy_min_out == 0 {
            return Err(anyhow!("buy_min_out=0 (cannot build safe arb)"));
        }

        // Sell leg: min SOL out
        let sell_min_out = sell_quote
            .amount_out
            .saturating_mul(10000 - slippage_bps as u64)
            / 10000;

        // Build buy instructions
        let buy_connector = self
            .dexes
            .get(&buy_dex)
            .ok_or_else(|| anyhow!("Unknown buy DEX: {}", buy_dex))?;

        // Approximate exact-out: spend the smallest SOL amount that should yield >= buy_min_out,
        // bounded by trade_amount (max capital).
        let buy_amount_in = self
            .estimate_min_amount_in_for_target_out(
                buy_connector,
                SOL_MINT,
                token_mint,
                buy_min_out,
                trade_amount,
                buy_quote.amount_out,
            )
            .await?
            .ok_or_else(|| anyhow!("cannot estimate buy amount_in for buy_min_out"))?;

        let buy_instructions =
            buy_connector.build_swap_ix(SOL_MINT, token_mint, buy_amount_in, buy_min_out)?;

        // Build sell instructions
        let sell_connector = self
            .dexes
            .get(&sell_dex)
            .ok_or_else(|| anyhow!("Unknown sell DEX: {}", sell_dex))?;

        // IMPORTANT: sell the guaranteed minimum tokens (buy_min_out), not the optimistic
        // quoted output. Otherwise the second leg may fail due to insufficient token balance.
        let sell_instructions =
            sell_connector.build_swap_ix(token_mint, SOL_MINT, buy_min_out, sell_min_out)?;

        // Estimate compute units
        let total_cu = 200_000 * 2; // ~200k per swap

        Ok(CrossDexSwapPlan {
            buy_instructions,
            sell_instructions,
            total_compute_units: total_cu,
            buy_dex,
            sell_dex,
            input_sol_lamports: buy_amount_in,
            expected_output_sol_lamports: sell_quote.amount_out,
        })
    }

    /// Identify which DEX a pool belongs to based on address pattern
    fn identify_dex(&self, pool_address: &str) -> Result<(String, String)> {
        // For now, use simple heuristics based on pool address length and pattern
        // In production: query on-chain owner or use cached pool registry

        // Raydium AMM pools typically have specific patterns
        // PumpFun bonding curves are derived from mint
        // Orca whirlpools have their own pattern

        // Simple heuristic: check if pool exists in our connectors
        // This is a placeholder - real implementation would query chain

        let _pubkey = Pubkey::from_str(pool_address)
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
    #[test]
    fn test_is_cross_dex_arb_intent() {
        // Would need mock TradeIntent for proper testing
    }
}
