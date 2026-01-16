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

use crate::execution::live_pool_cache::{CachedPoolState, LivePoolCache};
use crate::execution::quote_calculator;
use crate::ipc::TradeIntent;
use crate::solana::dex::meteora_dlmm::MeteoraDlmm;
use crate::solana::dex::orca::Orca;
use crate::solana::dex::pumpfun::PumpFunDex;
use crate::solana::dex::pumpfun_amm::PumpFunAmmDex;
use crate::solana::dex::raydium::Raydium;
use crate::solana::dex::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;

/// Token-2022 Program ID
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

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

/// Determine the token program for a given mint.
/// Priority order:
/// 1. Intent-provided token_program (from TradeResources) - highest trust
/// 2. LivePoolCache (from Geyser TokenMintInfo) - cached from Geyser
/// 3. DEX hint (pump.fun always uses SPL Token) - static knowledge
/// 4. Default to SPL Token (most common case)
fn get_token_program_for_mint_cached(
    cache: Option<&crate::execution::live_pool_cache::LivePoolCache>,
    mint: &Pubkey,
    dex_hint: Option<&str>,
    intent_token_program: Option<&str>,
) -> spl_token::solana_program::pubkey::Pubkey {
    let token_2022_id = Pubkey::from_str(TOKEN_2022_PROGRAM_ID).unwrap_or_default();
    
    // Priority 1: Use intent-provided token_program (GEYSER-FIRST: arb-strategy already has this info)
    if let Some(prog_str) = intent_token_program {
        if let Ok(prog) = Pubkey::from_str(prog_str) {
            if prog == token_2022_id {
                info!(
                    mint = %mint,
                    token_program = %prog_str,
                    "Using Token-2022 program from Intent"
                );
                return spl_token::solana_program::pubkey::Pubkey::new_from_array(token_2022_id.to_bytes());
            } else {
                debug!(mint = %mint, "Using SPL Token program from Intent");
                return spl_token::id();
            }
        }
    }
    
    // Priority 2: Try cache (Geyser-populated)
    if let Some(c) = cache {
        if let Some(prog) = c.get_mint_program(mint) {
            if prog == token_2022_id {
                debug!(mint = %mint, "Mint uses Token-2022 program (from cache)");
                return spl_token::solana_program::pubkey::Pubkey::new_from_array(token_2022_id.to_bytes());
            } else {
                return spl_token::id();
            }
        }
    }

    // Priority 3: DEX hint (pump.fun/pumpfun/pump_amm always use SPL Token)
    if let Some(dex) = dex_hint {
        let dex_lower = dex.to_lowercase();
        if dex_lower.contains("pump") || dex_lower == "pumpfun" || dex_lower == "pump_amm" {
            return spl_token::id();
        }
    }

    // Priority 4: Default to SPL Token (most common case)
    debug!(mint = %mint, "Token program not in cache and not in Intent, defaulting to SPL Token");
    spl_token::id()
}

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
    /// Live pool cache for fresh Geyser data (Option C: reduces RPC calls)
    pool_cache: Option<Arc<LivePoolCache>>,
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
            pool_cache: None,
        }
    }

    /// Set RPC URL for DEXes that need direct HTTP access
    pub fn with_rpc_url(mut self, url: String) -> Self {
        self.rpc_url = Some(url);
        self
    }

    /// Set LivePoolCache for fresh Geyser data (Option C)
    pub fn with_pool_cache(mut self, cache: Arc<LivePoolCache>) -> Self {
        self.pool_cache = Some(cache);
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

    /// Try to inject pool state from LivePoolCache into DEX connector.
    /// Returns true if cache hit and injection successful.
    fn try_inject_from_cache(&self, dex: &str, pool_pk: &Pubkey, connector: &Arc<dyn Dex>) -> bool {
        let Some(ref cache) = self.pool_cache else {
            return false;
        };

        let Some(state) = cache.get(pool_pk) else {
            debug!(
                pool = %pool_pk,
                dex = %dex,
                "LivePoolCache miss"
            );
            return false;
        };

        match (dex, state) {
            ("meteora_dlmm", CachedPoolState::Meteora(meteora_state)) => {
                // Downcast to MeteoraDlmm and inject
                // Note: This requires the connector to be the concrete type
                // For now, we use set_pool_from_accounts with the cached data
                let accounts = vec![
                    pool_pk.to_string(),
                    meteora_state.token_x_mint.to_string(),
                    meteora_state.token_y_mint.to_string(),
                    meteora_state.reserve_x.to_string(),
                    meteora_state.reserve_y.to_string(),
                    format!("active_id:{}", meteora_state.active_id),
                    format!("bin_step:{}", meteora_state.bin_step),
                ];
                if connector.set_pool_from_accounts(&pool_pk.to_string(), &accounts).is_ok() {
                    info!(
                        pool = %pool_pk,
                        dex = %dex,
                        active_id = meteora_state.active_id,
                        "Injected pool state from LivePoolCache (no RPC)"
                    );
                    return true;
                }
            }
            ("orca", CachedPoolState::Orca(orca_state)) => {
                let mut accounts = vec![
                    pool_pk.to_string(),
                    orca_state.token_mint_a.to_string(),
                    orca_state.token_mint_b.to_string(),
                    orca_state.token_vault_a.to_string(),
                    orca_state.token_vault_b.to_string(),
                    format!("sqrt_price:{}", orca_state.sqrt_price),
                    format!("tick_current_index:{}", orca_state.tick_current_index),
                    format!("tick_spacing:{}", orca_state.tick_spacing),
                ];
                // CRITICAL: Pass cached token programs to avoid RPC in hot path!
                if let Some(prog_a) = orca_state.token_a_program {
                    accounts.push(format!("token_a_program:{}", prog_a));
                }
                if let Some(prog_b) = orca_state.token_b_program {
                    accounts.push(format!("token_b_program:{}", prog_b));
                }
                if connector.set_pool_from_accounts(&pool_pk.to_string(), &accounts).is_ok() {
                    info!(
                        pool = %pool_pk,
                        dex = %dex,
                        tick = orca_state.tick_current_index,
                        token_a_program = ?orca_state.token_a_program,
                        token_b_program = ?orca_state.token_b_program,
                        "Injected pool state from LivePoolCache (no RPC)"
                    );
                    return true;
                }
            }
            ("raydium", CachedPoolState::RaydiumAmm(_)) => {
                // Raydium uses inject_cached_amm_state via tx_builder
                // For cross_dex_handler, we just mark as cache available
                debug!(
                    pool = %pool_pk,
                    dex = %dex,
                    "Raydium pool in cache (injection via tx_builder)"
                );
                return true;
            }
            ("pumpfun", CachedPoolState::PumpFun(pf_state)) => {
                // PumpFun bonding curve - cache the creator in the DEX connector
                // The connector needs the creator to build swap instructions without RPC
                if pf_state.creator != Pubkey::default() {
                    // Try to downcast and cache the creator
                    // Note: We can't directly access PumpFunDex through Arc<dyn Dex>
                    // So we store the creator in a side map that we'll use later
                    info!(
                        pool = %pool_pk,
                        dex = %dex,
                        creator = %pf_state.creator,
                        token_mint = %pf_state.token_mint,
                        "PumpFun pool in cache with creator"
                    );
                    return true;
                }
            }
            _ => {
                debug!(
                    pool = %pool_pk,
                    dex = %dex,
                    "Cache state type mismatch or unsupported DEX"
                );
            }
        }

        false
    }

    /// Get PumpFun creator from LivePoolCache for a bonding curve
    /// Returns None if not found or not a PumpFun pool
    fn get_pumpfun_creator_from_cache(&self, bonding_curve: &Pubkey) -> Option<Pubkey> {
        self.pool_cache.as_ref()?.get_pumpfun_creator(bonding_curve)
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

        // Estimate token amount from price
        // buy_price = SOL per token (e.g., 0.00492)
        // trade_amount = lamports (e.g., 100_000_000 = 0.1 SOL)
        // tokens_ui = (trade_amount / 1e9) / buy_price = SOL_amount / buy_price
        // Most pump/meme tokens use 6 decimals, assume this as default
        let token_decimals = 6i32;
        let sol_amount = trade_amount as f64 / 1e9;
        let estimated_tokens = if buy_price > 0.0 {
            let tokens_ui = sol_amount / buy_price;
            (tokens_ui * 10f64.powi(token_decimals)) as u64
        } else {
            0
        };

        // Estimate SOL output from selling tokens (in lamports)
        // sell_price = SOL per token
        // sol_out_ui = tokens_ui * sell_price
        // sol_out_lamports = sol_out_ui * 1e9
        let estimated_sol_out = if sell_price > 0.0 && estimated_tokens > 0 {
            let tokens_ui = estimated_tokens as f64 / 10f64.powi(token_decimals);
            let sol_out_ui = tokens_ui * sell_price;
            (sol_out_ui * 1e9) as u64
        } else {
            0
        };
        
        debug!(
            buy_price,
            sell_price,
            trade_amount,
            sol_amount,
            estimated_tokens,
            estimated_sol_out,
            token_decimals,
            "Cross-DEX quote calculation (assuming {} decimals)", token_decimals
        );

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

        // =======================================================================
        // FRESH QUOTE FROM LIVEPOOL CACHE (Option C - no stale price-based quotes)
        // =======================================================================
        // The buy_quote from validation may be stale (computed from price metadata).
        // We compute a fresh quote from LivePoolCache if available.
        // This uses actual pool reserves, not just price.
        let buy_min_out = if let Some(ref cache) = self.pool_cache {
            // Try to get fresh quote from LivePoolCache
            let buy_pool_pk = Pubkey::from_str(&pools[0]).ok();
            let fresh_min_out = buy_pool_pk.and_then(|pk| {
                let (state, _slot, age_ms) = cache.get_with_metadata(&pk)?;
                
                // Don't use stale data (>10 seconds)
                if age_ms > 10_000 {
                    debug!(
                        pool = %pk,
                        age_ms,
                        "LivePoolCache too stale for fresh quote, using validation quote"
                    );
                    return None;
                }
                
                // Calculate quote based on DEX type and pool state
                let amount_out = match state {
                    CachedPoolState::Meteora(ref s) => {
                        // Meteora DLMM uses BINS with concentrated liquidity, NOT constant product.
                        // Our constant product approximation is optimistic - add extra buffer.
                        // reserve_y = SOL reserve, reserve_x = Token reserve
                        let reserve_in = s.reserve_y_balance? as u128;
                        let reserve_out = s.reserve_x_balance? as u128;
                        if reserve_in == 0 || reserve_out == 0 {
                            return None;
                        }
                        // Constant product approximation with ~30bps fee
                        let amount_in = trade_amount as u128;
                        let amount_after_fee = amount_in * 9970 / 10000;
                        let k = reserve_in * reserve_out;
                        let new_reserve_in = reserve_in + amount_after_fee;
                        let new_reserve_out = k / new_reserve_in;
                        let raw_out = reserve_out.saturating_sub(new_reserve_out) as u64;
                        // DLMM bins cause more slippage than constant product - apply 15% extra buffer
                        // This is because active bins may have less liquidity than reserves suggest
                        Some(raw_out.saturating_mul(85) / 100)
                    }
                    CachedPoolState::Orca(ref s) => {
                        // Orca Whirlpool uses concentrated liquidity (like DLMM)
                        // Constant product is an approximation - add extra buffer
                        let reserve_in = s.vault_b_balance? as u128; // SOL
                        let reserve_out = s.vault_a_balance? as u128; // Token
                        if reserve_in == 0 || reserve_out == 0 {
                            return None;
                        }
                        let amount_in = trade_amount as u128;
                        let amount_after_fee = amount_in * 9970 / 10000;
                        let k = reserve_in * reserve_out;
                        let new_reserve_in = reserve_in + amount_after_fee;
                        let new_reserve_out = k / new_reserve_in;
                        let raw_out = reserve_out.saturating_sub(new_reserve_out) as u64;
                        // Concentrated liquidity causes more slippage - apply 10% extra buffer
                        Some(raw_out.saturating_mul(90) / 100)
                    }
                    _ => None, // Other DEXes use validation quote
                }?;
                
                // Apply slippage
                let min_out = quote_calculator::apply_slippage(amount_out, slippage_bps);
                info!(
                    pool = %pk,
                    dex = %buy_dex,
                    trade_amount,
                    amount_out,
                    min_out,
                    age_ms,
                    validation_amount_out = buy_quote.amount_out,
                    "Fresh buy quote from LivePoolCache (replaces stale validation quote)"
                );
                Some(min_out)
            });
            
            match fresh_min_out {
                Some(min_out) if min_out > 0 => min_out,
                _ => {
                    // Fallback to validation quote
                    buy_quote
                        .amount_out
                        .saturating_mul(10000 - slippage_bps as u64)
                        / 10000
                }
            }
        } else {
            // No cache available, use validation quote
            buy_quote
                .amount_out
                .saturating_mul(10000 - slippage_bps as u64)
                / 10000
        };

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
            // No accounts in intent - check LivePoolCache
            // GEYSER-FIRST (TARGET_ARCHITECTURE.md §4.5): NO RPC in hot path!
            // If Geyser hasn't delivered the data, RPC won't have it either (same validator).
            let pool_pk = Pubkey::from_str(&buy_pool)
                .map_err(|_| anyhow!("Invalid buy pool address: {}", buy_pool))?;
            
            if !self.try_inject_from_cache(&buy_dex, &pool_pk, buy_connector) {
                // Cache miss = REJECT. No RPC fallback.
                // The arb-strategy should not have generated this intent without DexPoolAccounts.
                return Err(anyhow!(
                    "GEYSER_CACHE_MISS: buy pool {} ({}) not in cache and no accounts in intent. \
                     arb-strategy should require DexPoolAccounts before generating intents.",
                    buy_pool, buy_dex
                ));
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
        //
        // IMPORTANT: For Token-2022 mints, we MUST use the Token-2022 program ID,
        // otherwise ATA creation fails with IncorrectProgramId.
        // ====================================================================
        let mut ata_creation_instructions = Vec::new();
        if let Some(wallet) = self.wallet_pubkey {
            let wallet_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(wallet.to_bytes());
            
            // 1. Create WSOL ATA (WSOL always uses SPL Token, not Token-2022)
            let wsol_mint_pk = Pubkey::from_str(SOL_MINT)
                .map_err(|_| anyhow!("Invalid WSOL mint"))?;
            let wsol_mint_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(wsol_mint_pk.to_bytes());
            
            let create_wsol_ata_ix_prog = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &wallet_spl,        // payer
                &wallet_spl,        // owner  
                &wsol_mint_spl,     // WSOL mint
                &spl_token::id(),   // token program (WSOL is always SPL Token)
            );
            ata_creation_instructions.push(prog_ix_to_sdk(create_wsol_ata_ix_prog));
            
            // 2. Create Token ATA (for the token being bought/sold)
            //    MUST use the correct token program (SPL Token OR Token-2022)
            let token_mint_pk = Pubkey::from_str(token_mint)
                .map_err(|_| anyhow!("Invalid token mint: {}", token_mint))?;
            let token_mint_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(token_mint_pk.to_bytes());
            
            // Determine the token program:
            // 1. From Intent (arb-strategy sends this via TokenMintInfo)
            // 2. From cache (Geyser-populated)
            // 3. From DEX hint
            // 4. Default to SPL Token
            let token_program = get_token_program_for_mint_cached(
                self.pool_cache.as_deref(),
                &token_mint_pk,
                Some(&buy_dex), // Use buy_dex as hint for pump.fun detection
                intent.resources.token_program.as_deref(), // From Intent (GEYSER-FIRST)
            );
            
            let create_token_ata_ix_prog = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &wallet_spl,        // payer
                &wallet_spl,        // owner  
                &token_mint_spl,    // token mint
                &token_program,     // token program (dynamically determined!)
            );
            let token_ata_ix = prog_ix_to_sdk(create_token_ata_ix_prog);
            
            // Log the actual instruction accounts for debugging Token-2022 issues
            info!(
                token_mint = %token_mint,
                wsol_mint = %SOL_MINT,
                wallet = %wallet,
                token_program = %Pubkey::new_from_array(token_program.to_bytes()),
                token_ata_ix_accounts_count = token_ata_ix.accounts.len(),
                token_ata_ix_last_account = %token_ata_ix.accounts.last().map(|a| a.pubkey.to_string()).unwrap_or_default(),
                "Token ATA creation instruction built"
            );
            
            ata_creation_instructions.push(token_ata_ix);
            
            // ====================================================================
            // WRAP SOL: Transfer native SOL → WSOL ATA and sync
            //
            // DEXes like Meteora DLMM and Orca require WSOL (SPL token) in the ATA,
            // not native SOL. We must:
            // 1. Transfer native SOL from wallet to WSOL ATA
            // 2. Call SyncNative to update the WSOL balance
            // ====================================================================
            let wsol_ata = spl_associated_token_account::get_associated_token_address(
                &wallet_spl,
                &wsol_mint_spl,
            );
            let wsol_ata_sdk = Pubkey::new_from_array(wsol_ata.to_bytes());
            
            // Transfer native SOL to WSOL ATA
            // System program transfer: 4-byte discriminator (2u32) + 8-byte lamports
            let system_program_id = Pubkey::from_str("11111111111111111111111111111111")
                .expect("valid system program id");
            let transfer_ix = Instruction {
                program_id: system_program_id,
                accounts: vec![
                    AccountMeta {
                        pubkey: wallet,
                        is_signer: true,
                        is_writable: true,
                    },
                    AccountMeta {
                        pubkey: wsol_ata_sdk,
                        is_signer: false,
                        is_writable: true,
                    },
                ],
                data: {
                    // System transfer instruction: 4-byte enum index (2u32 LE) + 8-byte lamports (u64 LE)
                    let mut d = Vec::with_capacity(4 + 8);
                    d.extend_from_slice(&2u32.to_le_bytes()); // Transfer discriminator (4 bytes!)
                    d.extend_from_slice(&buy_amount_in.to_le_bytes());
                    d
                },
            };
            ata_creation_instructions.push(transfer_ix);
            
            // Sync native balance to WSOL token balance
            let sync_native_ix_prog = spl_token::instruction::sync_native(
                &spl_token::id(),
                &wsol_ata,
            ).map_err(|e| anyhow!("Failed to create sync_native instruction: {}", e))?;
            ata_creation_instructions.push(prog_ix_to_sdk(sync_native_ix_prog));
            
            info!(
                wsol_ata = %wsol_ata_sdk,
                amount = buy_amount_in,
                "Added wrap SOL instructions (transfer + sync_native)"
            );
        }

        // ====================================================================
        // GEYSER-FIRST: Inject cached creators for PumpFun before building IX
        // This eliminates RPC calls in build_swap_ix_async
        // ====================================================================
        let token_mint_pk = Pubkey::from_str(token_mint)
            .map_err(|_| anyhow!("Invalid token mint: {}", token_mint))?;
        
        // For PumpFun bonding curve: derive bonding_curve and get creator from cache
        if buy_dex == "pumpfun" {
            let (bonding_curve, _) = crate::solana::dex::pumpfun::PumpFunDex::derive_bonding_curve_static(&token_mint_pk);
            if let Some(creator) = self.get_pumpfun_creator_from_cache(&bonding_curve) {
                buy_connector.cache_extra_data(
                    &format!("creator:{}", token_mint),
                    &creator.to_string(),
                );
                debug!(
                    token_mint = %token_mint,
                    creator = %creator,
                    "Injected PumpFun creator from LivePoolCache for buy (GEYSER-FIRST)"
                );
            }
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
            // No accounts in intent - check LivePoolCache
            // GEYSER-FIRST (TARGET_ARCHITECTURE.md §4.5): NO RPC in hot path!
            // If Geyser hasn't delivered the data, RPC won't have it either (same validator).
            let pool_pk = Pubkey::from_str(&sell_pool)
                .map_err(|_| anyhow!("Invalid sell pool address: {}", sell_pool))?;
            
            if !self.try_inject_from_cache(&sell_dex, &pool_pk, sell_connector) {
                // Cache miss = REJECT. No RPC fallback.
                // The arb-strategy should not have generated this intent without DexPoolAccounts.
                return Err(anyhow!(
                    "GEYSER_CACHE_MISS: sell pool {} ({}) not in cache and no accounts in intent. \
                     arb-strategy should require DexPoolAccounts before generating intents.",
                    sell_pool, sell_dex
                ));
            }
        }

        // ====================================================================
        // GEYSER-FIRST: Inject cached creators for PumpFun SELL before building IX
        // ====================================================================
        if sell_dex == "pumpfun" {
            let (bonding_curve, _) = crate::solana::dex::pumpfun::PumpFunDex::derive_bonding_curve_static(&token_mint_pk);
            if let Some(creator) = self.get_pumpfun_creator_from_cache(&bonding_curve) {
                sell_connector.cache_extra_data(
                    &format!("creator:{}", token_mint),
                    &creator.to_string(),
                );
                debug!(
                    token_mint = %token_mint,
                    creator = %creator,
                    "Injected PumpFun creator from LivePoolCache for sell (GEYSER-FIRST)"
                );
            }
        }

        // IMPORTANT: sell the guaranteed minimum tokens (buy_min_out), not the optimistic
        // quoted output. Otherwise the second leg may fail due to insufficient token balance.
        let sell_instructions = sell_connector
            .build_swap_ix_async(token_mint, SOL_MINT, buy_min_out, sell_min_out)
            .await?;

        // Combine: ATA creation + wrap SOL (if any) + buy swap + sell swap
        // Instructions in ata_creation_instructions:
        // - 2x CreateIdempotent ATA (~20k CU each)
        // - 1x System Transfer (~300 CU)
        // - 1x SyncNative (~1k CU)
        let _ata_count = ata_creation_instructions.len();
        let mut combined_buy_instructions = ata_creation_instructions;
        combined_buy_instructions.extend(buy_instructions);

        // Estimate compute units:
        // - 2x ATA create ~25k each = 50k
        // - 1x Transfer + SyncNative ~2k
        // - 2x swap ~200k each = 400k
        // Total ~452k, round up to 500k for safety
        let total_cu = 500_000_u32;

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
