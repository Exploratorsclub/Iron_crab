//! Cross-DEX Arbitrage Handler
//!
//! **ARCHITECTURE (TARGET_ARCHITECTURE.md Section 4.2):**
//!
//! - Pool Discovery gehört NUR in market-data (Data Plane)
//! - execution-engine macht KEINE RPC-basierte Pool Discovery
//! - Quote-Berechnungen werden von arb-strategy gemacht (hat Geyser-Daten)
//! - execution-engine verwendet Intent-Daten für Validierung
//! - Nur `build_swap_ix()` darf RPC verwenden (für Instruction Building)
//!
//! Handles arbitrage intents by:
//! 1. Validating intent data (TTL, spread from arb-strategy)
//! 2. Building swap instructions using DEX connectors
//! 3. Creating atomic Jito bundle
//!
//! **NICHT** zuständig für:
//! - Pool Discovery (das macht market-data via Geyser)
//! - Quote-Berechnung (das macht arb-strategy)
//! - RPC getProgramAccounts calls

use anyhow::{anyhow, Result};
use solana_sdk::{instruction::Instruction, pubkey::Pubkey};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::ipc::TradeIntent;
use crate::solana::dex::meteora_dlmm::MeteoraDlmm;
use crate::solana::dex::orca::Orca;
use crate::solana::dex::pumpfun::PumpFunDex;
use crate::solana::dex::pumpfun_amm::PumpFunAmmDex;
use crate::solana::dex::raydium::Raydium;
use crate::solana::dex::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;

/// SOL mint address
pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Minimum spread in bps to execute (from intent metadata, no re-quote!)
const MIN_EXECUTION_SPREAD_BPS: i64 = 30; // 0.3% minimum

/// Result of Cross-DEX arbitrage validation
/// 
/// NOTE: buy_quote/sell_quote are constructed from Intent metadata,
/// NOT from RPC calls. arb-strategy is the source of truth for quotes.
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
/// **ARCHITECTURE COMPLIANCE:**
/// - Does NOT do pool discovery (that's market-data's job via Geyser)
/// - Does NOT call quote_exact_in() (that's arb-strategy's job with Geyser data)
/// - ONLY validates intent metadata and builds swap instructions
/// - DEX connectors are used ONLY for build_swap_ix()
pub struct CrossDexHandler {
    rpc: Arc<SolanaRpc>,
    /// DEX connectors by name - used ONLY for build_swap_ix()
    dexes: HashMap<String, Arc<dyn Dex>>,
    /// Wallet pubkey (used as user authority / payer)
    wallet_pubkey: Option<Pubkey>,
    /// Default slippage for swaps (bps)
    default_slippage_bps: u32,
    /// RPC URL for DEXes that need it directly
    rpc_url: Option<String>,
}

impl CrossDexHandler {
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
    /// 
    /// NOTE: These connectors are used ONLY for build_swap_ix().
    /// They should NOT be used for quote_exact_in() or pool discovery.
    /// Pool data comes from arb-strategy via Intent metadata.
    pub async fn init_dexes(&mut self) -> Result<()> {
        // Initialize Raydium - for build_swap_ix() only
        let mut raydium = Raydium::new(Arc::clone(&self.rpc));
        if let Some(pk) = self.wallet_pubkey {
            raydium.set_user_authority(pk);
        }
        self.dexes.insert("raydium".to_string(), Arc::new(raydium));
        info!("Initialized Raydium DEX connector (for IX building only)");

        // Initialize PumpFun (Bonding Curve) - for build_swap_ix() only
        let mut pumpfun = PumpFunDex::new(Arc::clone(&self.rpc))?;
        if let Some(pk) = self.wallet_pubkey {
            pumpfun.set_user_authority(pk);
        }
        self.dexes.insert("pumpfun".to_string(), Arc::new(pumpfun));
        info!("Initialized PumpFun DEX connector (for IX building only)");

        // Initialize PumpSwap AMM (pump_amm) - for build_swap_ix() only
        if let Some(ref rpc_url) = self.rpc_url {
            let mut pump_amm = PumpFunAmmDex::new(
                Arc::clone(&self.rpc),
                rpc_url.clone(),
                None,
            );
            if let Some(pk) = self.wallet_pubkey {
                pump_amm.set_user_authority(pk);
            }
            self.dexes
                .insert("pump_amm".to_string(), Arc::new(pump_amm));
            info!("Initialized PumpSwap AMM connector (for IX building only)");
        } else {
            warn!("PumpSwap AMM not initialized: RPC URL not provided");
        }

        // Initialize Meteora DLMM - for build_swap_ix() only
        let mut meteora = MeteoraDlmm::new(Arc::clone(&self.rpc));
        if let Some(pk) = self.wallet_pubkey {
            meteora.set_user_authority(pk);
        }
        self.dexes
            .insert("meteora_dlmm".to_string(), Arc::new(meteora));
        info!("Initialized Meteora DLMM connector (for IX building only)");

        // Initialize Orca Whirlpool - for build_swap_ix() only
        let orca = Orca::new(Arc::clone(&self.rpc));
        if let Some(pk) = self.wallet_pubkey {
            orca.set_user_authority(pk);
        }
        self.dexes.insert("orca".to_string(), Arc::new(orca));
        info!("Initialized Orca Whirlpool connector (for IX building only)");

        Ok(())
    }

    /// Check if this intent is a cross-DEX arbitrage intent
    pub fn is_cross_dex_arb_intent(intent: &TradeIntent) -> bool {
        intent.resources.pools.len() == 2 && intent.source == "arb-strategy"
    }

    /// Validate a cross-DEX arbitrage opportunity using INTENT DATA ONLY
    ///
    /// **ARCHITECTURE COMPLIANCE (TARGET_ARCHITECTURE.md Section 4.2):**
    /// - NO RPC calls for quotes (arb-strategy already computed them via Geyser data)
    /// - Uses spread_bps, estimated_profit_lamports from intent metadata
    /// - Only validates: TTL, spread threshold, profit threshold
    ///
    /// arb-strategy is the SOURCE OF TRUTH for price/quote data.
    pub async fn validate_arb_opportunity(
        &self,
        intent: &TradeIntent,
        tx_cost_lamports: u64,
    ) -> Result<CrossDexValidation> {
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

        // Extract data from intent metadata (computed by arb-strategy via Geyser)
        let buy_dex = intent.metadata.get("buy_dex").cloned().unwrap_or_default();
        let sell_dex = intent.metadata.get("sell_dex").cloned().unwrap_or_default();
        
        // Parse spread and profit from intent - arb-strategy computed these
        let spread_bps: i64 = intent
            .metadata
            .get("spread_bps")
            .and_then(|s| s.parse().ok())
            .unwrap_or(intent.expected_roi_bps as i64);
            
        let estimated_profit: i64 = intent
            .metadata
            .get("estimated_profit_lamports")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Verify DEX names are known
        if !buy_dex.is_empty() && !self.dexes.contains_key(&buy_dex) {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: None,
                sell_quote: None,
                actual_spread_bps: spread_bps,
                estimated_profit_lamports: estimated_profit,
                reject_reason: Some(format!("Unknown buy DEX: {} (not registered for IX building)", buy_dex)),
            });
        }

        if !sell_dex.is_empty() && !self.dexes.contains_key(&sell_dex) {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: None,
                sell_quote: None,
                actual_spread_bps: spread_bps,
                estimated_profit_lamports: estimated_profit,
                reject_reason: Some(format!("Unknown sell DEX: {} (not registered for IX building)", sell_dex)),
            });
        }

        debug!(
            buy_dex = %buy_dex,
            sell_dex = %sell_dex,
            spread_bps = spread_bps,
            estimated_profit = estimated_profit,
            tx_cost = tx_cost_lamports,
            "Validating cross-DEX arb from intent metadata (no RPC re-quote)"
        );

        // Validate spread threshold
        if spread_bps < MIN_EXECUTION_SPREAD_BPS {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: None,
                sell_quote: None,
                actual_spread_bps: spread_bps,
                estimated_profit_lamports: estimated_profit,
                reject_reason: Some(format!(
                    "Spread too low: {}bps < {}bps minimum",
                    spread_bps, MIN_EXECUTION_SPREAD_BPS
                )),
            });
        }

        // Validate profit after tx costs
        let net_profit = estimated_profit - tx_cost_lamports as i64;
        if net_profit <= 0 {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: None,
                sell_quote: None,
                actual_spread_bps: spread_bps,
                estimated_profit_lamports: net_profit,
                reject_reason: Some(format!(
                    "Not profitable after tx costs: {} - {} = {} lamports",
                    estimated_profit, tx_cost_lamports, net_profit
                )),
            });
        }

        // Create placeholder quotes from intent metadata
        // These are used by build_swap_plan for slippage calculation
        let trade_amount = intent.required_capital.raw;
        let buy_price: f64 = intent
            .metadata
            .get("buy_price")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let sell_price: f64 = intent
            .metadata
            .get("sell_price")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        // Estimate token amount from price (SOL/token price -> tokens per SOL input)
        let estimated_tokens = if buy_price > 0.0 {
            (trade_amount as f64 / buy_price) as u64
        } else {
            0
        };

        // Estimate SOL output from selling tokens
        let estimated_sol_out = if sell_price > 0.0 {
            (estimated_tokens as f64 * sell_price) as u64
        } else {
            0
        };

        let buy_quote = Quote {
            amount_out: estimated_tokens,
            price_impact_bps: 0,
            route: vec![pools[0].clone()],
            fee_bps: 30, // Default estimate
            in_reserve: 0,
            out_reserve: 0,
            input_mint: SOL_MINT.to_string(),
            output_mint: intent.resources.output_mint.clone(),
            tick_spacing: None,
        };

        let sell_quote = Quote {
            amount_out: estimated_sol_out,
            price_impact_bps: 0,
            route: vec![pools[1].clone()],
            fee_bps: 30,
            in_reserve: 0,
            out_reserve: 0,
            input_mint: intent.resources.output_mint.clone(),
            output_mint: SOL_MINT.to_string(),
            tick_spacing: None,
        };

        info!(
            spread_bps = spread_bps,
            estimated_profit = estimated_profit,
            net_profit = net_profit,
            buy_dex = %buy_dex,
            sell_dex = %sell_dex,
            "Cross-DEX arb validation PASSED (using intent data, no RPC)"
        );

        Ok(CrossDexValidation {
            is_valid: true,
            buy_quote: Some(buy_quote),
            sell_quote: Some(sell_quote),
            actual_spread_bps: spread_bps,
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

        // Use metadata from arb-strategy (source of truth)
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

        // Get pool addresses from intent metadata (provided by arb-strategy)
        let buy_pool = intent.metadata.get("buy_pool").cloned().unwrap_or_default();
        let sell_pool = intent.metadata.get("sell_pool").cloned().unwrap_or_default();

        // Build buy instructions
        let buy_connector = self
            .dexes
            .get(&buy_dex)
            .ok_or_else(|| anyhow!("Unknown buy DEX: {}", buy_dex))?;

        // Pre-load the buy pool into connector cache (single getAccount, not getProgramAccounts)
        if !buy_pool.is_empty() {
            if let Ok(pool_pk) = Pubkey::from_str(&buy_pool) {
                if let Err(e) = buy_connector.load_pool_by_address(&pool_pk).await {
                    debug!(
                        pool = %buy_pool,
                        dex = %buy_dex,
                        error = %e,
                        "Failed to pre-load buy pool (will try build_swap_ix anyway)"
                    );
                }
            }
        }

        // Use trade_amount (from intent) as buy_amount_in
        // arb-strategy already computed the optimal amounts
        let buy_amount_in = trade_amount;

        let buy_instructions =
            buy_connector.build_swap_ix(SOL_MINT, token_mint, buy_amount_in, buy_min_out)?;

        // Build sell instructions
        let sell_connector = self
            .dexes
            .get(&sell_dex)
            .ok_or_else(|| anyhow!("Unknown sell DEX: {}", sell_dex))?;

        // Pre-load the sell pool into connector cache
        if !sell_pool.is_empty() {
            if let Ok(pool_pk) = Pubkey::from_str(&sell_pool) {
                if let Err(e) = sell_connector.load_pool_by_address(&pool_pk).await {
                    debug!(
                        pool = %sell_pool,
                        dex = %sell_dex,
                        error = %e,
                        "Failed to pre-load sell pool (will try build_swap_ix anyway)"
                    );
                }
            }
        }

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
