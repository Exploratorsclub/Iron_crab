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
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
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

/// Helper to convert spl_token instruction to solana_sdk instruction
fn prog_ix_to_sdk(ix: spl_token::solana_program::instruction::Instruction) -> Instruction {
    Instruction {
        program_id: Pubkey::new_from_array(ix.program_id.to_bytes()),
        accounts: ix
            .accounts
            .into_iter()
            .map(|m| AccountMeta {
                pubkey: Pubkey::new_from_array(m.pubkey.to_bytes()),
                is_signer: m.is_signer,
                is_writable: m.is_writable,
            })
            .collect(),
        data: ix.data,
    }
}

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
    
    /// Parse pool accounts from intent.resources.accounts
    /// 
    /// Format from arb-strategy:
    /// - "buy_pool_accounts_start:N" followed by N account strings
    /// - "sell_pool_accounts_start:M" followed by M account strings
    /// 
    /// Returns (buy_accounts, sell_accounts)
    fn parse_pool_accounts_from_intent(&self, accounts: &[String]) -> (Option<Vec<String>>, Option<Vec<String>>) {
        let mut buy_accounts: Option<Vec<String>> = None;
        let mut sell_accounts: Option<Vec<String>> = None;
        
        let mut i = 0;
        while i < accounts.len() {
            if let Some(rest) = accounts[i].strip_prefix("buy_pool_accounts_start:") {
                if let Ok(count) = rest.parse::<usize>() {
                    let end = (i + 1 + count).min(accounts.len());
                    buy_accounts = Some(accounts[i + 1..end].to_vec());
                    i = end;
                    continue;
                }
            }
            if let Some(rest) = accounts[i].strip_prefix("sell_pool_accounts_start:") {
                if let Ok(count) = rest.parse::<usize>() {
                    let end = (i + 1 + count).min(accounts.len());
                    sell_accounts = Some(accounts[i + 1..end].to_vec());
                    i = end;
                    continue;
                }
            }
            i += 1;
        }
        
        (buy_accounts, sell_accounts)
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
        
        // =======================================================================
        // Parse pool accounts from intent.resources.accounts (NO RPC!)
        // Format from arb-strategy: "buy_pool_accounts_start:N" followed by N accounts,
        // then "sell_pool_accounts_start:M" followed by M accounts.
        // =======================================================================
        let (buy_accounts, sell_accounts) = self.parse_pool_accounts_from_intent(&intent.resources.accounts);
        
        debug!(
            buy_pool = %buy_pool,
            sell_pool = %sell_pool,
            buy_accounts_count = buy_accounts.as_ref().map(|a| a.len()).unwrap_or(0),
            sell_accounts_count = sell_accounts.as_ref().map(|a| a.len()).unwrap_or(0),
            "Parsed pool accounts from intent"
        );

        // Build buy instructions
        let buy_connector = self
            .dexes
            .get(&buy_dex)
            .ok_or_else(|| anyhow!("Unknown buy DEX: {}", buy_dex))?;

        // Set pool from intent accounts (NO RPC!) - arb-strategy provides these from DexPoolAccounts events
        if let Some(ref accts) = buy_accounts {
            if let Err(e) = buy_connector.set_pool_from_accounts(&buy_pool, accts) {
                warn!(
                    pool = %buy_pool,
                    dex = %buy_dex,
                    error = %e,
                    "Failed to set buy pool from intent accounts"
                );
            }
        } else if !buy_pool.is_empty() {
            // No accounts in intent
            // For pump_amm: REJECT (DexPoolAccounts required per TARGET_ARCHITECTURE.md)
            // For meteora_dlmm/orca: Allow single getAccount fallback (acceptable per TARGET_ARCHITECTURE.md Section 4.2)
            if buy_dex == "pump_amm" {
                return Err(anyhow!(
                    "buy pool {} has no accounts in intent - pump_amm requires DexPoolAccounts",
                    buy_pool
                ));
            } else {
                // Single getAccount RPC fallback for Meteora/Orca (acceptable)
                info!(
                    pool = %buy_pool,
                    dex = %buy_dex,
                    "No accounts in intent for buy pool, using single getAccount RPC fallback"
                );
                let pool_pk = Pubkey::from_str(&buy_pool)
                    .map_err(|_| anyhow!("Invalid buy pool address: {}", buy_pool))?;
                match buy_connector.load_pool_by_address(&pool_pk).await {
                    Ok(()) => {
                        info!(
                            pool = %buy_pool,
                            dex = %buy_dex,
                            "Successfully loaded buy pool via RPC"
                        );
                    }
                    Err(e) => {
                        return Err(anyhow!("Failed to load buy pool {} via RPC: {}", buy_pool, e));
                    }
                }
            }
        }

        // Use trade_amount (from intent) as buy_amount_in
        // arb-strategy already computed the optimal amounts
        let buy_amount_in = trade_amount;

        // ====================================================================
        // Create ATAs for BOTH tokens involved in the swap (idempotent)
        // 
        // Orca Whirlpool requires ATAs for token_owner_account_a AND token_owner_account_b.
        // - token_owner_account_a = User's ATA for pool.token_mint_a (often WSOL)
        // - token_owner_account_b = User's ATA for pool.token_mint_b (the token)
        //
        // For SOL→Token swaps: We need WSOL ATA (for input) AND Token ATA (for output).
        // Without both, Orca fails with AnchorError AccountNotInitialized (3012).
        // ====================================================================
        let mut ata_creation_instructions = Vec::new();
        if let Some(wallet) = self.wallet_pubkey {
            let wallet_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(wallet.to_bytes());
            
            // 1. Create WSOL ATA (needed for Orca which uses WSOL, not native SOL)
            let wsol_mint_pk = Pubkey::from_str(SOL_MINT)
                .map_err(|_| anyhow!("Invalid WSOL mint"))?;
            let wsol_mint_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(wsol_mint_pk.to_bytes());
            
            let create_wsol_ata_ix_prog = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &wallet_spl,        // payer
                &wallet_spl,        // owner  
                &wsol_mint_spl,     // WSOL mint
                &spl_token::id(),   // token program
            );
            ata_creation_instructions.push(prog_ix_to_sdk(create_wsol_ata_ix_prog));
            
            // 2. Create Token ATA (for the token being bought/sold)
            let token_mint_pk = Pubkey::from_str(token_mint)
                .map_err(|_| anyhow!("Invalid token mint: {}", token_mint))?;
            let token_mint_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(token_mint_pk.to_bytes());
            
            let create_token_ata_ix_prog = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &wallet_spl,        // payer
                &wallet_spl,        // owner  
                &token_mint_spl,    // token mint
                &spl_token::id(),   // token program
            );
            ata_creation_instructions.push(prog_ix_to_sdk(create_token_ata_ix_prog));
            
            debug!(
                token_mint = %token_mint,
                wsol_mint = %SOL_MINT,
                wallet = %wallet,
                "Added idempotent ATA creation for WSOL and token"
            );
        }

        // Use async build to support DEXes that need it (PumpFun, etc.)
        let buy_instructions = buy_connector
            .build_swap_ix_async(SOL_MINT, token_mint, buy_amount_in, buy_min_out)
            .await?;

        // Build sell instructions
        let sell_connector = self
            .dexes
            .get(&sell_dex)
            .ok_or_else(|| anyhow!("Unknown sell DEX: {}", sell_dex))?;

        // Set sell pool from intent accounts (NO RPC!)
        if let Some(ref accts) = sell_accounts {
            if let Err(e) = sell_connector.set_pool_from_accounts(&sell_pool, accts) {
                warn!(
                    pool = %sell_pool,
                    dex = %sell_dex,
                    error = %e,
                    "Failed to set sell pool from intent accounts"
                );
            }
        } else if !sell_pool.is_empty() {
            // No accounts in intent
            // For pump_amm: REJECT (DexPoolAccounts required per TARGET_ARCHITECTURE.md)
            // For meteora_dlmm/orca: Allow single getAccount fallback (acceptable per TARGET_ARCHITECTURE.md Section 4.2)
            if sell_dex == "pump_amm" {
                return Err(anyhow!(
                    "sell pool {} has no accounts in intent - pump_amm requires DexPoolAccounts",
                    sell_pool
                ));
            } else {
                // Single getAccount RPC fallback for Meteora/Orca (acceptable)
                info!(
                    pool = %sell_pool,
                    dex = %sell_dex,
                    "No accounts in intent for sell pool, using single getAccount RPC fallback"
                );
                let pool_pk = Pubkey::from_str(&sell_pool)
                    .map_err(|_| anyhow!("Invalid sell pool address: {}", sell_pool))?;
                match sell_connector.load_pool_by_address(&pool_pk).await {
                    Ok(()) => {
                        info!(
                            pool = %sell_pool,
                            dex = %sell_dex,
                            "Successfully loaded sell pool via RPC"
                        );
                    }
                    Err(e) => {
                        return Err(anyhow!("Failed to load sell pool {} via RPC: {}", sell_pool, e));
                    }
                }
            }
        }

        // IMPORTANT: sell the guaranteed minimum tokens (buy_min_out), not the optimistic
        // quoted output. Otherwise the second leg may fail due to insufficient token balance.
        let sell_instructions = sell_connector
            .build_swap_ix_async(token_mint, SOL_MINT, buy_min_out, sell_min_out)
            .await?;

        // Combine: ATA creation (if any) + buy swap + sell swap
        // ATA creation is idempotent (safe if already exists) and costs ~20k CU each
        let ata_count = ata_creation_instructions.len();
        let mut combined_buy_instructions = ata_creation_instructions;
        combined_buy_instructions.extend(buy_instructions);

        // Estimate compute units (ATA create ~25k each + 2x swap ~200k each)
        let ata_cu = (ata_count as u32) * 25_000;
        let total_cu = ata_cu + 200_000 * 2;

        Ok(CrossDexSwapPlan {
            buy_instructions: combined_buy_instructions,
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
