//! Cross-DEX Arbitrage Handler
//!
//! **ARCHITECTURE (TARGET_ARCHITECTURE.md Section 4.2):**
//!
//! - Pool Discovery gehört NUR in market-data (Data Plane)
//! - execution-engine macht KEINE RPC-basierte Pool Discovery
//! - **GEYSER-FIRST: Quotes werden aus LivePoolCache berechnet (Geyser-fed)**
//! - arb-strategy liefert nur Pool-Adressen und DEX-Typ
//! - Nur `build_swap_ix()` darf RPC verwenden (für Instruction Building)
//!
//! Handles arbitrage intents by:
//! 1. **Computing fresh quotes from LivePoolCache (Geyser data)**
//! 2. Validating spread and profitability with fresh data
//! 3. Building swap instructions using DEX connectors
//! 4. Creating atomic Jito bundle
//!
//! **NICHT** zuständig für:
//! - Pool Discovery (das macht market-data via Geyser)
//! - RPC getProgramAccounts calls

use anyhow::{anyhow, Result};
use rust_decimal::Decimal;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::arbitrage::pool_quote::{
    is_expected_token_output_plausible, price_based_token_output_raw,
};
use crate::execution::live_pool_cache::{CachedPoolState, LivePoolCache};
use crate::ipc::TradeIntent;
use crate::solana::dex::meteora_dlmm::MeteoraDlmm;
use crate::solana::dex::meteora_swap_builder::MeteoraDlmmSwapBuilder;
use crate::solana::dex::orca::Orca;
use crate::solana::dex::pumpfun_amm::{
    pump_amm_buy_program_ix_index, pump_amm_canonical_global_config,
    pump_amm_normalize_v14_pool_accounts, pump_amm_resolve_sell_pre_fee_meta_1_for_build,
    pump_amm_sell_ix_uses_global_fee_at, pump_amm_singleton_global_volume_accumulator_pub,
    PumpFunAmmDex, PUMPFUN_AMM_BUY_EXT_FEE_CONFIG_IX, PUMPFUN_AMM_BUY_EXT_FEE_PROGRAM_IX,
    PUMPFUN_AMM_BUY_TOTAL_ACCOUNTS, PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS,
    PUMPFUN_AMM_SELL_EXTENDED_TOTAL_ACCOUNTS, PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS,
    PUMPFUN_AMM_SELL_FEE_CONFIG_IX_V2, PUMPFUN_AMM_SELL_FEE_PROGRAM_IX_V2,
};
use crate::solana::dex::raydium::Raydium;
use crate::solana::dex::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;

/// Token-2022 Program ID
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// PumpSwap AMM program id (for layout slot guards).
const PUMPFUN_AMM_PROGRAM_ID_STR: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

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
                return spl_token::solana_program::pubkey::Pubkey::new_from_array(
                    token_2022_id.to_bytes(),
                );
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
                return spl_token::solana_program::pubkey::Pubkey::new_from_array(
                    token_2022_id.to_bytes(),
                );
            } else {
                return spl_token::id();
            }
        }
    }

    if let Some(dex) = dex_hint {
        if dex == "pump_amm" {
            return spl_token::id();
        }
    }

    // Priority 4: Default to SPL Token (most common case)
    debug!(mint = %mint, "Token program not in cache and not in Intent, defaulting to SPL Token");
    spl_token::id()
}

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

/// Options for [`CrossDexHandler::build_swap_plan`] (bundle size / hot-path hints).
#[derive(Debug, Clone, Copy, Default)]
pub struct CrossDexPlanOptions {
    /// When true, omit token ATA `CreateIdempotent` — wallet snapshot proves ATA exists.
    pub skip_token_ata_create: bool,
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
        let mut raydium = Raydium::new_with_live_cache(
            Arc::clone(&self.rpc),
            self.pool_cache.clone(),
            false, // Arb = Hot Path — no RPC on vault reserve miss
        );
        if let Some(pk) = self.wallet_pubkey {
            raydium.set_user_authority(pk);
        }
        self.dexes.insert("raydium".to_string(), Arc::new(raydium));
        info!("Initialized Raydium DEX connector (for IX building only)");

        // PumpFun Bonding Curve: EXCLUDED from arb (arb-strategy rejects it).
        // Tokens on BC have no other pools → no arb opportunity. See arb_strategy.rs.

        // Initialize PumpSwap AMM (pump_amm) - for build_swap_ix() only
        if self.rpc_url.is_some() {
            let mut pump_amm = if let Some(ref cache) = self.pool_cache {
                PumpFunAmmDex::new_with_cache(Arc::clone(&self.rpc), Arc::clone(cache), false)
                // Arb = Hot Path — no RPC on cache miss. P3 #12.
            } else {
                PumpFunAmmDex::new(Arc::clone(&self.rpc))
            };
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
        let mut meteora = MeteoraDlmm::new_with_live_cache(
            Arc::clone(&self.rpc),
            self.pool_cache.clone(),
            false, // Arb = Hot Path — no RPC on vault reserve miss
        );
        if let Some(pk) = self.wallet_pubkey {
            meteora.set_user_authority(pk);
        }
        self.dexes
            .insert("meteora_dlmm".to_string(), Arc::new(meteora));
        info!("Initialized Meteora DLMM connector (for IX building only)");

        // Initialize Meteora CPMM - for build_swap_ix() only
        // Data comes from Geyser via set_pool_from_accounts, no RPC in hot path
        let mut meteora_cpmm = crate::solana::dex::meteora_cpmm::MeteoraCpmm::new();
        if let Some(pk) = self.wallet_pubkey {
            meteora_cpmm.set_user_authority(pk);
        }
        self.dexes
            .insert("meteora_cpmm".to_string(), Arc::new(meteora_cpmm));
        info!("Initialized Meteora CPMM connector (for IX building only)");

        // Initialize Orca Whirlpool - for build_swap_ix() only
        // Arb Cross-DEX = Hot Path: derive tick arrays from LivePoolCache, no RPC validation (I-7).
        // Parity with tx_builder: set_skip_tick_array_rpc_validation(!allow_rpc_fallback) where allow_rpc=false.
        let orca = Orca::new_with_cache(Arc::clone(&self.rpc), None, self.pool_cache.clone());
        orca.set_skip_tick_array_rpc_validation(true);
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
    fn parse_pool_accounts_from_intent(
        &self,
        accounts: &[String],
    ) -> (Option<Vec<String>>, Option<Vec<String>>) {
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

    /// Resolve pump_amm v14 `pool_accounts` from intent strings or LivePoolCache (GEYSER-FIRST, no RPC).
    fn resolve_pump_amm_pool_accounts_v14(
        &self,
        pool_str: &str,
        account_strings: Option<&[String]>,
    ) -> Result<Vec<Pubkey>> {
        let pool_pk = Pubkey::from_str(pool_str)
            .map_err(|_| anyhow!("Invalid pump_amm pool address: {}", pool_str))?;

        if let Some(accts) = account_strings {
            if accts.len() >= 14 {
                let mut parsed = Vec::with_capacity(14);
                for (idx, a) in accts.iter().take(14).enumerate() {
                    parsed.push(Pubkey::from_str(a).map_err(|e| {
                        anyhow!("invalid pump_amm pool account[{idx}] '{}': {e}", a)
                    })?);
                }
                if parsed[0] != pool_pk {
                    return Err(anyhow!(
                        "pump_amm pool mismatch: expected {pool_pk}, accounts[0]={}",
                        parsed[0]
                    ));
                }
                pump_amm_normalize_v14_pool_accounts(&pool_pk, &mut parsed);
                return Ok(parsed);
            }
        }

        if let Some(ref cache) = self.pool_cache {
            if let Some(CachedPoolState::PumpAmm(amm_state)) = cache.get(&pool_pk) {
                if amm_state.pool_accounts.len() >= 14 {
                    let mut accounts = amm_state.pool_accounts[..14].to_vec();
                    pump_amm_normalize_v14_pool_accounts(&pool_pk, &mut accounts);
                    return Ok(accounts);
                }
            }
        }

        Err(anyhow!(
            "pump_amm: pool {} requires 14 v14 accounts in intent or LivePoolCache",
            pool_str
        ))
    }

    /// Build pump_amm swap instructions via `build_swap_ix_from_pool_accounts_with_extended_tail`
    /// (same SSOT path as `tx_builder`, not deprecated `Dex::build_swap_ix`).
    #[allow(clippy::too_many_arguments)]
    fn build_pump_amm_arb_swap_ixs(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
        pool_str: &str,
        account_strings: Option<&[String]>,
        token_program_override: Option<Pubkey>,
        is_buy_leg: bool,
    ) -> Result<Vec<Instruction>> {
        let wallet = self
            .wallet_pubkey
            .ok_or_else(|| anyhow!("wallet_pubkey not set for pump_amm arb build"))?;

        let pool_pk = Pubkey::from_str(pool_str)?;
        let pool_accounts = self.resolve_pump_amm_pool_accounts_v14(pool_str, account_strings)?;

        let (
            sell_requires_cashback_remaining,
            sell_cashback_third_meta,
            sell_extended_tail_0,
            sell_extended_tail_1,
            sell_extended_fee_tail_0,
            sell_extended_fee_tail_1,
            sell_requires_pre_fee_metas,
            cached_sell_pre_fee_meta_1,
            sell_requires_fee_tail,
            sell_layout_ready,
        ) = if let Some(ref cache) = self.pool_cache {
            let (sell_ext, third, t0, t1) = cache.pump_amm_sell_extended_layout(&pool_pk);
            let (fee_t0, fee_t1) = cache.pump_amm_sell_fee_tail_layout(&pool_pk);
            (
                sell_ext,
                third,
                t0,
                t1,
                fee_t0,
                fee_t1,
                cache.pump_amm_sell_requires_pre_fee_metas(&pool_pk),
                cache.pump_amm_sell_pre_fee_meta_1(&pool_pk),
                cache.pump_amm_sell_requires_fee_tail(&pool_pk),
                cache.pump_amm_sell_layout_ready(&pool_pk),
            )
        } else {
            (
                false, None, None, None, None, None, false, None, false, false,
            )
        };

        let is_sell_leg = !is_buy_leg;
        if is_sell_leg
            && sell_requires_cashback_remaining
            && !sell_layout_ready
            && sell_cashback_third_meta.is_none()
        {
            return Err(anyhow!(
                "pump_amm SELL: extended layout required for pool {} but LivePoolCache layout not ready (missing third_meta/tails)",
                pool_str
            ));
        }
        if is_buy_leg
            && sell_requires_pre_fee_metas
            && sell_requires_cashback_remaining
            && !sell_layout_ready
        {
            return Err(anyhow!(
                "pump_amm BUY: extended v2 layout required for pool {} but LivePoolCache layout not ready",
                pool_str
            ));
        }
        if is_buy_leg
            && sell_requires_cashback_remaining
            && !sell_requires_pre_fee_metas
            && !sell_layout_ready
            && sell_cashback_third_meta.is_none()
        {
            return Err(anyhow!(
                "pump_amm BUY: extended layout required for pool {} but LivePoolCache layout not ready (missing third_meta/tails)",
                pool_str
            ));
        }

        let global_volume_accumulator = pool_accounts
            .get(11)
            .copied()
            .filter(|p| *p != Pubkey::default());
        let (sell_pre_fee_meta_1, _pre_fee_source) = if is_sell_leg && sell_requires_pre_fee_metas {
            if let Some(gva) = global_volume_accumulator {
                pump_amm_resolve_sell_pre_fee_meta_1_for_build(
                    &pool_pk,
                    gva,
                    cached_sell_pre_fee_meta_1,
                )
            } else {
                (cached_sell_pre_fee_meta_1, "cache_unvalidated")
            }
        } else if is_buy_leg && sell_requires_pre_fee_metas {
            pump_amm_resolve_sell_pre_fee_meta_1_for_build(
                &pool_pk,
                pump_amm_singleton_global_volume_accumulator_pub(),
                cached_sell_pre_fee_meta_1,
            )
        } else {
            (cached_sell_pre_fee_meta_1, "cache")
        };

        let use_extended_layout = sell_requires_cashback_remaining && (is_sell_leg || is_buy_leg);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts_with_extended_tail(
            input_mint,
            output_mint,
            amount_in,
            min_out,
            wallet,
            &pool_accounts,
            token_program_override,
            use_extended_layout,
            if use_extended_layout {
                sell_cashback_third_meta
            } else {
                None
            },
            sell_requires_pre_fee_metas && (is_sell_leg || is_buy_leg),
            sell_pre_fee_meta_1,
            sell_extended_tail_0,
            sell_extended_tail_1,
            sell_extended_fee_tail_0,
            sell_extended_fee_tail_1,
            sell_requires_fee_tail,
            false,
        )
        .map_err(|e| {
            anyhow!(
                "pump_amm {} leg build_swap_ix_from_pool_accounts failed for pool {}: {e}",
                if is_buy_leg { "BUY" } else { "SELL" },
                pool_str
            )
        })?;

        if let Some(ix) = ixs.first() {
            let account_count = ix.accounts.len();
            let canonical_gc = pump_amm_canonical_global_config();
            if ix.accounts.len() <= 2 || ix.accounts[2].pubkey != canonical_gc {
                return Err(anyhow!(
                    "pump_amm {} leg: global_config meta #2 mismatch for pool {}: got {}, expected {}",
                    if is_buy_leg { "BUY" } else { "SELL" },
                    pool_str,
                    ix.accounts
                        .get(2)
                        .map(|m| m.pubkey.to_string())
                        .unwrap_or_else(|| "<missing>".to_string()),
                    canonical_gc
                ));
            }
            let program_slot = if is_buy_leg {
                pump_amm_buy_program_ix_index(account_count)
            } else {
                Some(16)
            };
            let (fee_cfg_ix, fee_prog_ix) = if is_sell_leg {
                pump_amm_sell_ix_uses_global_fee_at(account_count).unwrap_or((0, 0))
            } else if account_count == PUMPFUN_AMM_BUY_TOTAL_ACCOUNTS {
                (20, 21)
            } else if account_count == PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS {
                (
                    PUMPFUN_AMM_SELL_FEE_CONFIG_IX_V2,
                    PUMPFUN_AMM_SELL_FEE_PROGRAM_IX_V2,
                )
            } else if account_count == PUMPFUN_AMM_SELL_EXTENDED_TOTAL_ACCOUNTS
                || account_count == PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS
            {
                (
                    PUMPFUN_AMM_BUY_EXT_FEE_CONFIG_IX,
                    PUMPFUN_AMM_BUY_EXT_FEE_PROGRAM_IX,
                )
            } else {
                (0, 0)
            };
            let fee_cfg_pk = ix
                .accounts
                .get(fee_cfg_ix)
                .map(|m| m.pubkey.to_string())
                .unwrap_or_default();
            let fee_prog_pk = ix
                .accounts
                .get(fee_prog_ix)
                .map(|m| m.pubkey.to_string())
                .unwrap_or_default();
            let program_pk = program_slot
                .and_then(|idx| ix.accounts.get(idx))
                .map(|m| m.pubkey.to_string())
                .unwrap_or_default();
            info!(
                pool = %pool_str,
                leg = if is_buy_leg { "buy" } else { "sell" },
                ix_account_count = account_count,
                program_slot_pubkey = %program_pk,
                fee_config_slot_pubkey = %fee_cfg_pk,
                fee_program_slot_pubkey = %fee_prog_pk,
                sell_requires_pre_fee_metas,
                sell_requires_cashback_remaining,
                "pump_amm cross-dex swap ix built (tx_builder SSOT path)"
            );
            let expected_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID_STR).unwrap_or_default();
            if let Some(idx) = program_slot {
                if ix.accounts[idx].pubkey != expected_program {
                    warn!(
                        pool = %pool_str,
                        leg = if is_buy_leg { "buy" } else { "sell" },
                        program_slot = idx,
                        got = %ix.accounts[idx].pubkey,
                        expected = %expected_program,
                        "pump_amm cross-dex: program meta slot mismatch before simulation"
                    );
                }
            }
        }

        Ok(ixs)
    }

    /// Check if this intent is a cross-DEX arbitrage intent (old 2-hop style)
    ///
    /// Returns false for multi-hop intents (which have swap_path populated),
    /// as those should go through tx_builder::build_multi_hop_tx_plan instead.
    pub fn is_cross_dex_arb_intent(intent: &TradeIntent) -> bool {
        // Multi-hop intents have swap_path - they use the new multi-hop tx plan builder
        if intent.swap_path.as_ref().is_some_and(|sp| !sp.is_empty()) {
            return false;
        }
        // Old 2-hop arb: pools.len() == 2 and source == arb-strategy
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
                if connector
                    .set_pool_from_accounts(&pool_pk.to_string(), &accounts)
                    .is_ok()
                {
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
                if connector
                    .set_pool_from_accounts(&pool_pk.to_string(), &accounts)
                    .is_ok()
                {
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
            ("meteora_cpmm", CachedPoolState::MeteoraCpmm(cpmm_state)) => {
                // Meteora CPMM - inject via set_pool_from_accounts
                let accounts = vec![
                    pool_pk.to_string(),
                    cpmm_state.token_0_mint.to_string(),
                    cpmm_state.token_1_mint.to_string(),
                    cpmm_state.token_0_vault.to_string(),
                    cpmm_state.token_1_vault.to_string(),
                    cpmm_state.amm_config.to_string(),
                    cpmm_state.observation_key.to_string(),
                    format!("reserve_0:{}", cpmm_state.reserve_0),
                    format!("reserve_1:{}", cpmm_state.reserve_1),
                ];
                if connector
                    .set_pool_from_accounts(&pool_pk.to_string(), &accounts)
                    .is_ok()
                {
                    info!(
                        pool = %pool_pk,
                        dex = %dex,
                        token_0 = %cpmm_state.token_0_mint,
                        token_1 = %cpmm_state.token_1_mint,
                        reserve_0 = cpmm_state.reserve_0,
                        reserve_1 = cpmm_state.reserve_1,
                        "Injected Meteora CPMM pool state from LivePoolCache (no RPC)"
                    );
                    return true;
                }
            }
            // pumpfun (BC) excluded from arb - see init_dexes
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

    /// Validate a cross-DEX arbitrage opportunity - OPTION B (SIMULATION-BASED)
    ///
    /// **ARCHITECTURE: Trust arb-strategy + simulation, minimal validation here**
    ///
    /// WHY WE SIMPLIFIED THIS (Option B):
    /// 1. arb-strategy already validated the opportunity with its own quotes
    /// 2. Cache reserves for DLMM/Orca are often incomplete (vault updates async)
    /// 3. The SIMULATION will tell us the real output → that's the true validation
    /// 4. Atomic bundles are all-or-nothing → no partial execution risk
    ///
    /// WHAT WE STILL CHECK:
    /// - Pool addresses are valid
    /// - DEX types are known
    /// - Pools exist in cache (presence only, not reserve values)
    ///
    /// The spread/profit check is SKIPPED here because:
    /// - arb-strategy already did it with fresh data
    /// - Our cache quote calculation was failing for DLMM (reserve_x_balance=None)
    /// - Simulation will show real profit anyway
    pub async fn validate_arb_opportunity(
        &self,
        intent: &TradeIntent,
        _tx_cost_lamports: u64,
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

        // Extract DEX types from intent metadata
        let buy_dex = intent.metadata.get("buy_dex").cloned().unwrap_or_default();
        let sell_dex = intent.metadata.get("sell_dex").cloned().unwrap_or_default();
        let buy_pool = &pools[0];
        let sell_pool = &pools[1];
        let trade_amount = intent.required_capital.raw;

        // Verify DEX names are known
        if !buy_dex.is_empty() && !self.dexes.contains_key(&buy_dex) {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: None,
                sell_quote: None,
                actual_spread_bps: 0,
                estimated_profit_lamports: 0,
                reject_reason: Some(format!("Unknown buy DEX: {}", buy_dex)),
            });
        }

        if !sell_dex.is_empty() && !self.dexes.contains_key(&sell_dex) {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: None,
                sell_quote: None,
                actual_spread_bps: 0,
                estimated_profit_lamports: 0,
                reject_reason: Some(format!("Unknown sell DEX: {}", sell_dex)),
            });
        }

        // ====================================================================
        // OPTION B: Minimal validation - trust arb-strategy + simulation
        // ====================================================================
        // We just check that pools exist in cache (presence only).
        // arb-strategy already validated spread with its own quotes.
        // Simulation will show true profit before we send.
        let cache = match &self.pool_cache {
            Some(c) => c,
            None => {
                return Ok(CrossDexValidation {
                    is_valid: false,
                    buy_quote: None,
                    sell_quote: None,
                    actual_spread_bps: 0,
                    estimated_profit_lamports: 0,
                    reject_reason: Some("LivePoolCache not available".to_string()),
                });
            }
        };

        // Parse pool addresses
        let buy_pool_pk = Pubkey::from_str(buy_pool)
            .map_err(|_| anyhow!("Invalid buy pool address: {}", buy_pool))?;
        let sell_pool_pk = Pubkey::from_str(sell_pool)
            .map_err(|_| anyhow!("Invalid sell pool address: {}", sell_pool))?;

        // Check pool presence in cache (NOT reserves - those may be incomplete for DLMM)
        let buy_in_cache = cache.get_with_metadata(&buy_pool_pk).is_some();
        let sell_in_cache = cache.get_with_metadata(&sell_pool_pk).is_some();

        if !buy_in_cache {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: None,
                sell_quote: None,
                actual_spread_bps: 0,
                estimated_profit_lamports: 0,
                reject_reason: Some(format!("Buy pool {} not in LivePoolCache", buy_pool)),
            });
        }

        if !sell_in_cache {
            return Ok(CrossDexValidation {
                is_valid: false,
                buy_quote: None,
                sell_quote: None,
                actual_spread_bps: 0,
                estimated_profit_lamports: 0,
                reject_reason: Some(format!("Sell pool {} not in LivePoolCache", sell_pool)),
            });
        }

        // Use spread/profit from arb-strategy intent (they computed it with fresh data)
        let strategy_spread_bps = intent
            .metadata
            .get("spread_bps")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let strategy_profit = intent
            .metadata
            .get("estimated_profit_lamports")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        // Get buy_price from intent metadata (SOL per token)
        // This is needed to compute expected token output for sell leg
        let buy_price_str = intent
            .metadata
            .get("buy_price")
            .cloned()
            .unwrap_or_else(|| "0".to_string());
        let buy_price: f64 = buy_price_str.parse().unwrap_or(0.0);

        info!(
            buy_dex = %buy_dex,
            sell_dex = %sell_dex,
            buy_pool = %buy_pool,
            sell_pool = %sell_pool,
            trade_amount,
            buy_price,
            strategy_spread_bps,
            strategy_profit,
            "OPTION B: Trusting arb-strategy spread (simulation will validate)"
        );

        // =========================================================================
        // OPTION D: Use expected_token_output from arb-strategy (calculated from reserves)
        // =========================================================================
        // arb-strategy calculates exact token output using:
        // - Cached pool reserves from Geyser (PoolStateUpdate events)
        // - Constant product formula with fee deduction
        //
        // This eliminates the need for safety margins because:
        // - The value is computed from the same Geyser data the simulation uses
        // - No stale price estimation, no guessing
        //
        // Fallback (if arb-strategy didn't provide it or value is implausible vs buy_price):
        // - Price-based estimation with 15% safety margin (for DLMM or missing reserves)
        let token_decimals = 6u8;
        let price_based_estimate = buy_price_str
            .parse::<Decimal>()
            .ok()
            .and_then(|p| price_based_token_output_raw(trade_amount, p, token_decimals));
        let expected_tokens_out: u64 = intent
            .metadata
            .get("expected_token_output")
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&token_out| {
                is_expected_token_output_plausible(token_out, price_based_estimate, trade_amount)
            })
            .inspect(|&token_out| {
                info!(
                    token_out,
                    price_based_estimate = ?price_based_estimate,
                    source = "arb-strategy (Option D)",
                    "Using expected_token_output from intent metadata (reserve-based)"
                );
            })
            .unwrap_or_else(|| {
                if let Some(ignored) = intent
                    .metadata
                    .get("expected_token_output")
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    warn!(
                        token_out = ignored,
                        price_based_estimate = ?price_based_estimate,
                        trade_amount,
                        buy_price,
                        "Ignoring implausible expected_token_output in intent metadata"
                    );
                }
                // Fallback: price-based estimation for DLMM or when reserves unavailable
                if buy_price > 0.0 {
                    let sol_amount = trade_amount as f64 / 1_000_000_000.0;
                    let tokens = sol_amount / buy_price;
                    let raw_tokens = (tokens * 1_000_000.0) as u64;
                    // 15% safety margin for price-based estimation
                    // DLMM bin concentration can cause significant deviation from price-based estimates
                    // This ensures sell_amount_in <= actual buy output to prevent "insufficient funds"
                    let with_safety = (raw_tokens as f64 * 0.85) as u64;
                    info!(
                        raw_tokens,
                        with_safety,
                        safety_margin_pct = 15,
                        source = "price-based (fallback)",
                        "Using price-based estimation (no expected_token_output in metadata)"
                    );
                    with_safety
                } else {
                    warn!(
                        buy_price,
                        trade_amount,
                        "buy_price missing or zero, using trade_amount as token estimate"
                    );
                    trade_amount
                }
            });

        // Build quotes for build_swap_plan
        // buy_quote.amount_out is used as sell_amount_in for the sell leg
        let buy_quote = Quote {
            amount_out: expected_tokens_out, // From Option D (reserves) or fallback (price-based)
            price_impact_bps: 0,
            route: vec![buy_pool.clone()],
            fee_bps: 30,
            in_reserve: 0,
            out_reserve: 0,
            input_mint: SOL_MINT.to_string(),
            output_mint: intent.resources.output_mint.clone(),
            tick_spacing: None,
        };

        let sell_quote = Quote {
            amount_out: (trade_amount as i64 + strategy_profit) as u64, // Expected output
            price_impact_bps: 0,
            route: vec![sell_pool.clone()],
            fee_bps: 30,
            in_reserve: 0,
            out_reserve: 0,
            input_mint: intent.resources.output_mint.clone(),
            output_mint: SOL_MINT.to_string(),
            tick_spacing: None,
        };

        Ok(CrossDexValidation {
            is_valid: true,
            buy_quote: Some(buy_quote),
            sell_quote: Some(sell_quote),
            actual_spread_bps: strategy_spread_bps,
            estimated_profit_lamports: strategy_profit,
            reject_reason: None,
        })
    }

    /// Compute BUY quote (SOL -> Token) from cached pool state
    /// Returns (tokens_out, reserve_sol, reserve_token) or None if cannot compute
    #[allow(dead_code)] // Kept for future use if we need cache-based quotes again
    fn compute_buy_quote_from_cache(
        &self,
        state: &CachedPoolState,
        _dex: &str,
        sol_in_lamports: u64,
    ) -> Option<(u64, u64, u64)> {
        let amount_in = sol_in_lamports as u128;

        match state {
            CachedPoolState::PumpAmm(s) => {
                let reserve_sol = s.quote_reserve? as u128; // quote = SOL
                let reserve_token = s.base_reserve? as u128; // base = Token
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // Constant product with 0.25% fee
                let amount_after_fee = amount_in * 9975 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_sol = reserve_sol + amount_after_fee;
                let new_reserve_token = k / new_reserve_sol;
                let tokens_out = reserve_token.saturating_sub(new_reserve_token) as u64;
                Some((tokens_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::Meteora(s) => {
                let reserve_sol = s.reserve_y_balance? as u128;
                let reserve_token = s.reserve_x_balance? as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // DLMM approximation with 0.30% fee + 20% buffer for bin concentration
                let amount_after_fee = amount_in * 9970 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_sol = reserve_sol + amount_after_fee;
                let new_reserve_token = k / new_reserve_sol;
                let raw_out = reserve_token.saturating_sub(new_reserve_token) as u64;
                let tokens_out = raw_out.saturating_mul(80) / 100; // 20% buffer for DLMM
                Some((tokens_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::Orca(s) => {
                let reserve_sol = s.vault_b_balance? as u128;
                let reserve_token = s.vault_a_balance? as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // Concentrated liquidity approximation with 0.30% fee + 15% buffer
                let amount_after_fee = amount_in * 9970 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_sol = reserve_sol + amount_after_fee;
                let new_reserve_token = k / new_reserve_sol;
                let raw_out = reserve_token.saturating_sub(new_reserve_token) as u64;
                let tokens_out = raw_out.saturating_mul(85) / 100; // 15% buffer
                Some((tokens_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::RaydiumAmm(s) => {
                let reserve_sol = s.pc_reserve.unwrap_or(0) as u128;
                let reserve_token = s.coin_reserve.unwrap_or(0) as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // Constant product with 0.25% fee
                let amount_after_fee = amount_in * 9975 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_sol = reserve_sol + amount_after_fee;
                let new_reserve_token = k / new_reserve_sol;
                let tokens_out = reserve_token.saturating_sub(new_reserve_token) as u64;
                Some((tokens_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::RaydiumCpmm(s) => {
                let reserve_sol = s.reserve_1.unwrap_or(0) as u128;
                let reserve_token = s.reserve_0.unwrap_or(0) as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // CPMM with 0.25% fee
                let amount_after_fee = amount_in * 9975 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_sol = reserve_sol + amount_after_fee;
                let new_reserve_token = k / new_reserve_sol;
                let tokens_out = reserve_token.saturating_sub(new_reserve_token) as u64;
                Some((tokens_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::PumpFun(s) => {
                // PumpFun bonding curve uses virtual reserves
                let reserve_sol = s.virtual_sol_reserves as u128;
                let reserve_token = s.virtual_token_reserves as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // Bonding curve with 1% fee
                let amount_after_fee = amount_in * 9900 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_sol = reserve_sol + amount_after_fee;
                let new_reserve_token = k / new_reserve_sol;
                let tokens_out = reserve_token.saturating_sub(new_reserve_token) as u64;
                Some((tokens_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::MeteoraCpmm(s) => {
                // Meteora CPMM: mint_0 = token, mint_1 = WSOL (SOL in, token out)
                let reserve_sol = s.reserve_1 as u128;
                let reserve_token = s.reserve_0 as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // CPMM with 0.25% fee
                let amount_after_fee = amount_in * 9975 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_sol = reserve_sol + amount_after_fee;
                let new_reserve_token = k / new_reserve_sol;
                let tokens_out = reserve_token.saturating_sub(new_reserve_token) as u64;
                Some((tokens_out, reserve_sol as u64, reserve_token as u64))
            }
        }
    }

    /// Compute SELL quote (Token -> SOL) from cached pool state
    /// Returns (sol_out, reserve_sol, reserve_token) or None if cannot compute
    #[allow(dead_code)] // Kept for future use if we need cache-based quotes again
    fn compute_sell_quote_from_cache(
        &self,
        state: &CachedPoolState,
        _dex: &str,
        tokens_in: u64,
    ) -> Option<(u64, u64, u64)> {
        let amount_in = tokens_in as u128;

        match state {
            CachedPoolState::PumpAmm(s) => {
                let reserve_token = s.base_reserve? as u128;
                let reserve_sol = s.quote_reserve? as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // Constant product with 0.25% fee
                let amount_after_fee = amount_in * 9975 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_token = reserve_token + amount_after_fee;
                let new_reserve_sol = k / new_reserve_token;
                let sol_out = reserve_sol.saturating_sub(new_reserve_sol) as u64;
                Some((sol_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::Meteora(s) => {
                let reserve_token = s.reserve_x_balance? as u128;
                let reserve_sol = s.reserve_y_balance? as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // DLMM approximation with 0.30% fee + 20% buffer
                let amount_after_fee = amount_in * 9970 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_token = reserve_token + amount_after_fee;
                let new_reserve_sol = k / new_reserve_token;
                let raw_out = reserve_sol.saturating_sub(new_reserve_sol) as u64;
                let sol_out = raw_out.saturating_mul(80) / 100;
                Some((sol_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::Orca(s) => {
                let reserve_token = s.vault_a_balance? as u128;
                let reserve_sol = s.vault_b_balance? as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // Concentrated liquidity approximation with 15% buffer
                let amount_after_fee = amount_in * 9970 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_token = reserve_token + amount_after_fee;
                let new_reserve_sol = k / new_reserve_token;
                let raw_out = reserve_sol.saturating_sub(new_reserve_sol) as u64;
                let sol_out = raw_out.saturating_mul(85) / 100;
                Some((sol_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::RaydiumAmm(s) => {
                let reserve_token = s.coin_reserve.unwrap_or(0) as u128;
                let reserve_sol = s.pc_reserve.unwrap_or(0) as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                let amount_after_fee = amount_in * 9975 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_token = reserve_token + amount_after_fee;
                let new_reserve_sol = k / new_reserve_token;
                let sol_out = reserve_sol.saturating_sub(new_reserve_sol) as u64;
                Some((sol_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::RaydiumCpmm(s) => {
                let reserve_token = s.reserve_0.unwrap_or(0) as u128;
                let reserve_sol = s.reserve_1.unwrap_or(0) as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                let amount_after_fee = amount_in * 9975 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_token = reserve_token + amount_after_fee;
                let new_reserve_sol = k / new_reserve_token;
                let sol_out = reserve_sol.saturating_sub(new_reserve_sol) as u64;
                Some((sol_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::PumpFun(s) => {
                let reserve_token = s.virtual_token_reserves as u128;
                let reserve_sol = s.virtual_sol_reserves as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                let amount_after_fee = amount_in * 9900 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_token = reserve_token + amount_after_fee;
                let new_reserve_sol = k / new_reserve_token;
                let sol_out = reserve_sol.saturating_sub(new_reserve_sol) as u64;
                Some((sol_out, reserve_sol as u64, reserve_token as u64))
            }
            CachedPoolState::MeteoraCpmm(s) => {
                // Meteora CPMM: mint_0 = token, mint_1 = WSOL (token in, SOL out)
                let reserve_token = s.reserve_0 as u128;
                let reserve_sol = s.reserve_1 as u128;
                if reserve_sol == 0 || reserve_token == 0 {
                    return None;
                }
                // CPMM with 0.25% fee
                let amount_after_fee = amount_in * 9975 / 10000;
                let k = reserve_sol * reserve_token;
                let new_reserve_token = reserve_token + amount_after_fee;
                let new_reserve_sol = k / new_reserve_token;
                let sol_out = reserve_sol.saturating_sub(new_reserve_sol) as u64;
                Some((sol_out, reserve_sol as u64, reserve_token as u64))
            }
        }
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
        options: CrossDexPlanOptions,
    ) -> Result<CrossDexSwapPlan> {
        // buy_quote.amount_out is the expected token output from buy leg
        // This is used as amount_in for the sell leg
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

        // =======================================================================
        // ATOMIC ARB BUNDLES: MINIMAL min_out (Option B)
        // =======================================================================
        // For atomic arb bundles, we use min_out=1 instead of calculating from stale quotes.
        //
        // WHY THIS IS SAFE:
        // 1. Arb bundles are ATOMIC - both legs execute or neither does
        // 2. We SIMULATE before sending - simulation shows real output
        // 3. Spread-check already validated profitability with fresh quotes
        // 4. The only "slippage" risk is MEV, but we use Jito bundles (atomic)
        //
        // WHY THIS FIXES SIM_FAILED (Error 6003):
        // - Cache data can be 100-5000ms old
        // - Between cache read and simulation, price moves
        // - Old approach: calculate min_out from stale cache → too high → SIM_FAILED
        // - New approach: min_out=1 → simulation succeeds → we see real output
        //
        // The simulation output tells us the real profit. If it's negative,
        // the sell leg would fail anyway (output < input for that direction).
        let buy_min_out: u64 = 1;
        let sell_min_out: u64 = 1;

        info!(
            buy_dex = %buy_dex,
            sell_dex = %sell_dex,
            buy_min_out,
            sell_min_out,
            "Using minimal min_out for atomic arb bundle (Option B - simulation validates output)"
        );

        // Get pool addresses from intent metadata (needed for instruction building)
        let buy_pool = intent.metadata.get("buy_pool").cloned().unwrap_or_default();
        let sell_pool = intent
            .metadata
            .get("sell_pool")
            .cloned()
            .unwrap_or_default();

        // Debug: log pool addresses for troubleshooting
        info!(
            buy_pool = %buy_pool,
            sell_pool = %sell_pool,
            buy_pool_empty = buy_pool.is_empty(),
            sell_pool_empty = sell_pool.is_empty(),
            "Pool addresses from intent metadata"
        );

        // =======================================================================
        // Parse pool accounts from intent.resources.accounts (NO RPC!)
        // Format from arb-strategy: "buy_pool_accounts_start:N" followed by N accounts,
        // then "sell_pool_accounts_start:M" followed by M accounts.
        // =======================================================================
        let (buy_accounts, sell_accounts) =
            self.parse_pool_accounts_from_intent(&intent.resources.accounts);

        debug!(
            buy_pool = %buy_pool,
            sell_pool = %sell_pool,
            buy_accounts_count = buy_accounts.as_ref().map(|a| a.len()).unwrap_or(0),
            sell_accounts_count = sell_accounts.as_ref().map(|a| a.len()).unwrap_or(0),
            "Parsed pool accounts from intent"
        );

        // Use trade_amount (from intent) as buy_amount_in
        let buy_amount_in = trade_amount;

        // ====================================================================
        // Determine token program BEFORE ATA creation and DEX injection
        // ====================================================================
        let token_mint_pk = Pubkey::from_str(token_mint)
            .map_err(|_| anyhow!("Invalid token mint: {}", token_mint))?;

        let token_program = get_token_program_for_mint_cached(
            self.pool_cache.as_deref(),
            &token_mint_pk,
            Some(&buy_dex),
            intent.resources.token_program.as_deref(),
        );
        let token_program_sdk = Pubkey::new_from_array(token_program.to_bytes());

        // ====================================================================
        // Create Token ATA (idempotent) - only when wallet ATA is not yet known
        // ====================================================================
        let mut ata_creation_instructions = Vec::new();
        if options.skip_token_ata_create {
            debug!(
                token_mint = %token_mint,
                "Skipping token ATA CreateIdempotent (wallet snapshot proves ATA exists)"
            );
            crate::metrics::record_arb_bundle_ata_create_skipped_known_ata();
        } else if let Some(wallet) = self.wallet_pubkey {
            let wallet_spl =
                spl_token::solana_program::pubkey::Pubkey::new_from_array(wallet.to_bytes());

            let token_mint_spl =
                spl_token::solana_program::pubkey::Pubkey::new_from_array(token_mint_pk.to_bytes());

            let create_token_ata_ix_prog = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &wallet_spl,
                &wallet_spl,
                &token_mint_spl,
                &token_program,
            );
            let token_ata_ix = prog_ix_to_sdk(create_token_ata_ix_prog);

            debug!(
                token_mint = %token_mint,
                wallet = %wallet,
                token_program = %token_program_sdk,
                "Token ATA creation instruction built (WSOL ATA handled by WsolManager)"
            );

            ata_creation_instructions.push(token_ata_ix);
        }

        // Build buy instructions
        let mut buy_swap_instructions = if buy_dex == "pump_amm" {
            self.build_pump_amm_arb_swap_ixs(
                SOL_MINT,
                token_mint,
                buy_amount_in,
                buy_min_out,
                &buy_pool,
                buy_accounts.as_deref(),
                Some(token_program_sdk),
                true,
            )?
        } else {
            let buy_connector = self
                .dexes
                .get(&buy_dex)
                .ok_or_else(|| anyhow!("Unknown buy DEX: {}", buy_dex))?;

            if buy_dex != "pump_amm" && !buy_pool.is_empty() {
                buy_connector.cache_extra_data("pool_address_hint", &buy_pool);
            }

            if let Some(ref accts) = buy_accounts {
                buy_connector
                    .set_pool_from_accounts(&buy_pool, accts)
                    .map_err(|e| {
                        anyhow!(
                            "Failed to set buy pool from intent accounts (pool={}, dex={}): {}",
                            buy_pool,
                            buy_dex,
                            e
                        )
                    })?;
            } else if !buy_pool.is_empty() {
                let pool_pk = Pubkey::from_str(&buy_pool)
                    .map_err(|_| anyhow!("Invalid buy pool address: {}", buy_pool))?;

                if !self.try_inject_from_cache(&buy_dex, &pool_pk, buy_connector) {
                    return Err(anyhow!(
                        "GEYSER_CACHE_MISS: buy pool {} ({}) not in cache and no accounts in intent. \
                         arb-strategy should require DexPoolAccounts before generating intents.",
                        buy_pool,
                        buy_dex
                    ));
                }
            }

            buy_connector.cache_extra_data(
                &format!("token_program:{}", token_mint),
                &token_program_sdk.to_string(),
            );

            buy_connector
                .build_swap_ix_async(SOL_MINT, token_mint, buy_amount_in, buy_min_out)
                .await?
        };

        if buy_dex == "meteora_dlmm" {
            if let Ok(pool_pk) = Pubkey::from_str(&buy_pool) {
                let has_bitmap_extension = self
                    .pool_cache
                    .as_ref()
                    .and_then(|cache| cache.meteora_dlmm_has_bitmap_extension(&pool_pk));
                for ix in &mut buy_swap_instructions {
                    MeteoraDlmmSwapBuilder::patch_swap_ix_bitmap_extension(
                        ix,
                        &pool_pk,
                        has_bitmap_extension,
                    )?;
                }
            }
        }

        // Build sell instructions
        let sell_amount_in = buy_quote.amount_out;

        info!(
            sell_amount_in,
            buy_quote_amount_out = buy_quote.amount_out,
            sell_min_out,
            "Building sell instruction with expected buy output as amount_in (Option D)"
        );

        let sell_instructions = if sell_dex == "pump_amm" {
            self.build_pump_amm_arb_swap_ixs(
                token_mint,
                SOL_MINT,
                sell_amount_in,
                sell_min_out,
                &sell_pool,
                sell_accounts.as_deref(),
                Some(token_program_sdk),
                false,
            )?
        } else {
            let sell_connector = self
                .dexes
                .get(&sell_dex)
                .ok_or_else(|| anyhow!("Unknown sell DEX: {}", sell_dex))?;

            if sell_dex != "pump_amm" && !sell_pool.is_empty() {
                sell_connector.cache_extra_data("pool_address_hint", &sell_pool);
            }

            sell_connector.cache_extra_data(
                &format!("token_program:{}", token_mint),
                &token_program_sdk.to_string(),
            );

            if let Some(ref accts) = sell_accounts {
                sell_connector
                    .set_pool_from_accounts(&sell_pool, accts)
                    .map_err(|e| {
                        anyhow!(
                            "Failed to set sell pool from intent accounts (pool={}, dex={}): {}",
                            sell_pool,
                            sell_dex,
                            e
                        )
                    })?;
            } else if !sell_pool.is_empty() {
                let pool_pk = Pubkey::from_str(&sell_pool)
                    .map_err(|_| anyhow!("Invalid sell pool address: {}", sell_pool))?;

                if !self.try_inject_from_cache(&sell_dex, &pool_pk, sell_connector) {
                    return Err(anyhow!(
                        "GEYSER_CACHE_MISS: sell pool {} ({}) not in cache and no accounts in intent. \
                         arb-strategy should require DexPoolAccounts before generating intents.",
                        sell_pool,
                        sell_dex
                    ));
                }
            }

            sell_connector
                .build_swap_ix_async(token_mint, SOL_MINT, sell_amount_in, sell_min_out)
                .await?
        };

        let mut combined_buy_instructions = ata_creation_instructions;
        combined_buy_instructions.extend(buy_swap_instructions);

        // Estimate compute units:
        // - 1x Token ATA create ~20k (often no-op ~2k)
        // - 2x swap ~200k each = 400k
        // Total ~420k, round up to 450k for safety
        let total_cu = 450_000_u32;

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
            Err(anyhow!(
                "pumpfun bonding curve excluded from arb (no other pools to arb against)"
            ))
        } else {
            // Default to Raydium
            warn!(pool = %pool_address, "Could not identify DEX, defaulting to Raydium");
            Ok(("raydium".to_string(), pool_address.to_string()))
        }
    }

    /// Get a specific DEX connector by name (for tests and debugging).
    pub fn get_dex(&self, name: &str) -> Option<Arc<dyn Dex>> {
        self.dexes.get(name).cloned()
    }

    /// Get all DEX connectors as a Vec (for Router construction)
    pub fn get_all_dexes(&self) -> Vec<Arc<dyn Dex>> {
        self.dexes.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::live_pool_cache::{
        CachedPoolState, LivePoolCache, MeteoraState, OrcaWhirlpoolState, PumpAmmState,
    };
    use crate::ipc::{
        ExplicitAmount, IntentOrigin, IntentTier, RecordHeader, TradeIntent, TradeResources,
        TradeSide, TradingRegime,
    };
    use crate::solana::address_lookup_table::{
        analyze_versioned_message_alt_usage, compile_v0_versioned_message, get_common_accounts,
        loaded_addresses_from_alt_lookups, resolved_instruction_account_pubkey,
        versioned_message_duplicate_static_keys, AltCompileAnalysis, LoadedAlt,
    };
    use crate::solana::compute_budget_helper;
    use crate::solana::dex::orca::ORCA_WHIRLPOOL_PROGRAM;
    use crate::solana::dex::pumpfun_amm::{
        pump_amm_buy_program_ix_index, pump_amm_canonical_global_config,
        PUMPFUN_AMM_BUILD_SWAP_FEE_CONFIG_STR, PUMPFUN_AMM_BUILD_SWAP_FEE_PROGRAM_STR,
        PUMPFUN_AMM_GLOBAL_CONFIG_STR, PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS,
        PUMPFUN_AMM_SELL_EXTENDED_TOTAL_ACCOUNTS,
    };
    use crate::solana::dex::router::Router;
    use crate::solana::dex::Quote;
    use crate::solana::rpc::SolanaRpc;
    use crate::storage::locks::LockManager;
    use solana_sdk::hash::Hash;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Keypair;
    use solana_sdk::signer::Signer;
    use solana_sdk::transaction::VersionedTransaction;
    use std::collections::HashMap;
    use std::str::FromStr;

    const MAX_SERIALIZED_TX_BYTES: usize = 1232;
    const JITO_TIP_ACCOUNT: &str = "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5";

    fn versioned_tx_serialized_len(tx: &VersionedTransaction) -> usize {
        bincode::serialize(tx)
            .expect("serialize versioned tx")
            .len()
    }

    fn make_pump_amm_cache_with_reserves(
        pool_market: Pubkey,
        base_mint: Pubkey,
        base_reserve: u64,
        quote_reserve: u64,
    ) -> Arc<LivePoolCache> {
        let cache = LivePoolCache::new();
        cache.upsert(
            pool_market,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint: Pubkey::from_str(SOL_MINT).unwrap(),
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(base_reserve),
                quote_reserve: Some(quote_reserve),
                pool_accounts: vec![],
                creator: None,
            }),
            100,
        );
        Arc::new(cache)
    }

    fn pump_amm_v14_pool_accounts(pool_market: Pubkey, base_mint: Pubkey) -> Vec<Pubkey> {
        vec![
            pool_market,
            Pubkey::from_str("ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw").unwrap(),
            base_mint,
            Pubkey::from_str(SOL_MINT).unwrap(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_BUILD_SWAP_FEE_CONFIG_STR).unwrap(),
            Pubkey::from_str(PUMPFUN_AMM_BUILD_SWAP_FEE_PROGRAM_STR).unwrap(),
        ]
    }

    fn seed_prod_like_cross_dex_cache(
        cache: &LivePoolCache,
        buy_pool: Pubkey,
        sell_pool: Pubkey,
        token_mint: Pubkey,
    ) {
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        cache.upsert(
            buy_pool,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: sol_mint,
                token_y_mint: token_mint,
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: -281,
                bin_step: 10,
                reserve_x_balance: Some(50_000_000_000),
                reserve_y_balance: Some(1_000_000_000_000),
            }),
            100,
        );
        cache.set_meteora_dlmm_has_bitmap_extension(&buy_pool, true);
        let sell_pool_accounts = pump_amm_v14_pool_accounts(sell_pool, token_mint);
        cache.upsert(
            sell_pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: token_mint,
                quote_mint: sol_mint,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1_000_000_000_000),
                quote_reserve: Some(50_000_000_000),
                pool_accounts: sell_pool_accounts,
                creator: None,
            }),
            100,
        );
        let tail_0 = Pubkey::from_str("5CXzL4rxA677oGhKUDBxxapLk6wLpRPLmackZDHGmZCQ").unwrap();
        let tail_1 = Pubkey::from_str("5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD").unwrap();
        let third = Pubkey::from_str("HjQjngTDqoHE6aaGhUqfz9aQ7WZcBRjy5xB8PScLSr8i").unwrap();
        cache.merge_pump_amm_sell_extended_layout(
            &sell_pool,
            true,
            Some(third),
            Some(tail_0),
            Some(tail_1),
            None,
            None,
            false,
            false,
            None,
        );
        cache.set_pump_amm_sell_layout_ready(&sell_pool, true);
    }

    fn cross_dex_test_intent(
        token_mint: &Pubkey,
        buy_pool: &Pubkey,
        sell_pool: &Pubkey,
    ) -> TradeIntent {
        let mut metadata = HashMap::new();
        metadata.insert("buy_dex".to_string(), "meteora_dlmm".to_string());
        metadata.insert("sell_dex".to_string(), "pump_amm".to_string());
        metadata.insert("buy_pool".to_string(), buy_pool.to_string());
        metadata.insert("sell_pool".to_string(), sell_pool.to_string());
        metadata.insert("spread_bps".to_string(), "50".to_string());
        metadata.insert(
            "estimated_profit_lamports".to_string(),
            "500000".to_string(),
        );

        let buy_accounts: Vec<String> = [
            buy_pool.to_string(),
            SOL_MINT.to_string(),
            token_mint.to_string(),
            Pubkey::new_unique().to_string(),
            Pubkey::new_unique().to_string(),
            "active_id:-281".to_string(),
            "bin_step:10".to_string(),
        ]
        .into_iter()
        .collect();
        let sell_accounts: Vec<String> = pump_amm_v14_pool_accounts(*sell_pool, *token_mint)
            .into_iter()
            .map(|p| p.to_string())
            .collect();
        let mut accounts = Vec::new();
        accounts.push(format!("buy_pool_accounts_start:{}", buy_accounts.len()));
        accounts.extend(buy_accounts);
        accounts.push(format!("sell_pool_accounts_start:{}", sell_accounts.len()));
        accounts.extend(sell_accounts);

        TradeIntent {
            header: RecordHeader {
                schema_version: 1,
                ts_unix_ms: 1,
                component: "test".to_string(),
                build: "test".to_string(),
                run_id: "run".to_string(),
            },
            intent_id: "arb-test-size".to_string(),
            source: "arb-strategy".to_string(),
            tier: IntentTier::Arb,
            origin_type: IntentOrigin::StrategyA,
            deadline_slot: None,
            ttl_ms: Some(30_000),
            required_capital: ExplicitAmount {
                raw: 100_000_000,
                decimals: 9,
                ui: None,
            },
            resources: TradeResources {
                input_mint: SOL_MINT.to_string(),
                output_mint: token_mint.to_string(),
                pools: vec![buy_pool.to_string(), sell_pool.to_string()],
                accounts,
                token_program: Some(spl_token::id().to_string()),
            },
            expected_roi_bps: 50,
            max_slippage_bps: 500,
            side: TradeSide::Buy,
            regime: TradingRegime::NotApplicable,
            trigger_event_id: None,
            require_bundle: Some(true),
            bundle_tip_lamports: Some(10_000),
            hint_compute_units: None,
            hint_priority_fee_micro_lamports: None,
            hint_urgency: None,
            metadata,
            execution: None,
            swap_path: None,
        }
    }

    fn cross_dex_validation_fixture(token_mint: &Pubkey) -> CrossDexValidation {
        CrossDexValidation {
            is_valid: true,
            buy_quote: Some(Quote {
                amount_out: 1_000_000,
                price_impact_bps: 0,
                route: vec![],
                fee_bps: 0,
                in_reserve: 0,
                out_reserve: 0,
                input_mint: SOL_MINT.to_string(),
                output_mint: token_mint.to_string(),
                tick_spacing: None,
            }),
            sell_quote: Some(Quote {
                amount_out: 101_000_000,
                price_impact_bps: 0,
                route: vec![],
                fee_bps: 0,
                in_reserve: 0,
                out_reserve: 0,
                input_mint: token_mint.to_string(),
                output_mint: SOL_MINT.to_string(),
                tick_spacing: None,
            }),
            actual_spread_bps: 50,
            estimated_profit_lamports: 500_000,
            reject_reason: None,
        }
    }

    async fn build_prod_pattern_cross_dex_plan(
        skip_token_ata: bool,
    ) -> (CrossDexSwapPlan, Keypair, Pubkey) {
        let wallet = Keypair::new();
        let token_mint = Pubkey::new_unique();
        let buy_pool = Pubkey::from_str("2TD1fMPg2w7Hjt8bASSdxi92YFNQFgvdznqVApe3NGpn").unwrap();
        let sell_pool = Pubkey::new_unique();

        let cache = Arc::new(LivePoolCache::new());
        seed_prod_like_cross_dex_cache(&cache, buy_pool, sell_pool, token_mint);

        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let mut handler = CrossDexHandler::new(rpc.clone(), Some(wallet.pubkey()))
            .with_rpc_url("http://127.0.0.1:0".to_string())
            .with_pool_cache(cache);
        handler.init_dexes().await.expect("init_dexes");

        let intent = cross_dex_test_intent(&token_mint, &buy_pool, &sell_pool);
        let validation = cross_dex_validation_fixture(&token_mint);
        let plan = handler
            .build_swap_plan(
                &intent,
                &validation,
                CrossDexPlanOptions {
                    skip_token_ata_create: skip_token_ata,
                },
            )
            .await
            .expect("build_swap_plan");
        (plan, wallet, token_mint)
    }

    fn compile_cross_dex_bundle_tx_with_alt(
        wallet: &Keypair,
        plan: &CrossDexSwapPlan,
        alt_accounts: Vec<Pubkey>,
    ) -> (VersionedTransaction, usize, usize, AltCompileAnalysis) {
        let tip = Pubkey::from_str(JITO_TIP_ACCOUNT).unwrap();
        let mut ixs = Vec::new();
        ixs.push(compute_budget_helper::set_compute_unit_limit(450_000));
        ixs.push(compute_budget_helper::set_compute_unit_price(1_000));
        ixs.extend(plan.buy_instructions.iter().cloned());
        ixs.extend(plan.sell_instructions.iter().cloned());
        let mut tip_data = vec![2, 0, 0, 0];
        tip_data.extend_from_slice(&10_000u64.to_le_bytes());
        ixs.push(Instruction {
            program_id: Pubkey::from_str("11111111111111111111111111111111").unwrap(),
            accounts: vec![
                AccountMeta::new(wallet.pubkey(), true),
                AccountMeta::new(tip, false),
            ],
            data: tip_data,
        });

        let alt = LoadedAlt {
            address: Pubkey::new_unique(),
            accounts: alt_accounts,
        };
        let blockhash = Hash::new_unique();
        let message =
            compile_v0_versioned_message(&wallet.pubkey(), &ixs, Some(&alt), Some(&tip), blockhash)
                .expect("compile v0 message");
        let tx = VersionedTransaction::try_new(message, &[wallet]).expect("sign tx");
        let analysis = analyze_versioned_message_alt_usage(&tx.message, Some(&alt));
        let size = versioned_tx_serialized_len(&tx);
        (tx, size, analysis.alt_hit_count, analysis)
    }

    /// Optimistic ALT: every instruction account is in the table (not prod-realistic).
    fn compile_cross_dex_bundle_tx_optimistic_alt(
        wallet: &Keypair,
        plan: &CrossDexSwapPlan,
    ) -> (VersionedTransaction, usize, usize, AltCompileAnalysis) {
        let tip = Pubkey::from_str(JITO_TIP_ACCOUNT).unwrap();
        let mut ixs = Vec::new();
        ixs.push(compute_budget_helper::set_compute_unit_limit(450_000));
        ixs.push(compute_budget_helper::set_compute_unit_price(1_000));
        ixs.extend(plan.buy_instructions.iter().cloned());
        ixs.extend(plan.sell_instructions.iter().cloned());
        let mut tip_data = vec![2, 0, 0, 0];
        tip_data.extend_from_slice(&10_000u64.to_le_bytes());
        ixs.push(Instruction {
            program_id: Pubkey::from_str("11111111111111111111111111111111").unwrap(),
            accounts: vec![
                AccountMeta::new(wallet.pubkey(), true),
                AccountMeta::new(tip, false),
            ],
            data: tip_data,
        });
        let mut alt_pubkeys = get_common_accounts();
        for ix in &ixs {
            alt_pubkeys.push(ix.program_id);
            for meta in &ix.accounts {
                alt_pubkeys.push(meta.pubkey);
            }
        }
        alt_pubkeys.sort();
        alt_pubkeys.dedup();
        compile_cross_dex_bundle_tx_with_alt(wallet, plan, alt_pubkeys)
    }

    /// Prod-realistic ALT: only `COMMON_ACCOUNTS` globals (EE loads on-chain ALT, no runtime merge).
    fn compile_cross_dex_bundle_tx_realistic_alt(
        wallet: &Keypair,
        plan: &CrossDexSwapPlan,
    ) -> (VersionedTransaction, usize, usize, AltCompileAnalysis) {
        compile_cross_dex_bundle_tx_with_alt(wallet, plan, get_common_accounts())
    }

    fn compile_cross_dex_bundle_tx(
        wallet: &Keypair,
        plan: &CrossDexSwapPlan,
    ) -> (VersionedTransaction, usize, usize) {
        let (tx, size, alt_hits, _) = compile_cross_dex_bundle_tx_optimistic_alt(wallet, plan);
        (tx, size, alt_hits)
    }

    #[tokio::test]
    async fn cross_dex_ata_skip_omits_create_idempotent_instruction() {
        let (with_ata, _, _) = build_prod_pattern_cross_dex_plan(false).await;
        let (without_ata, _, _) = build_prod_pattern_cross_dex_plan(true).await;
        assert!(
            with_ata.buy_instructions.len() > without_ata.buy_instructions.len(),
            "ATA skip must remove at least one buy instruction"
        );
        assert_eq!(
            without_ata.buy_instructions.len() + 1,
            with_ata.buy_instructions.len()
        );
    }

    #[test]
    fn lock_manager_wallet_snapshot_seen_implies_ata_known() {
        let lm = LockManager::new(1_000_000_000);
        let mint = "9Pfyn4Hvbg1z9y8K9f6K9f6K9f6K9f6K9f6K9f6pump";
        assert!(!lm.token_wallet_snapshot_seen(mint));
        lm.set_available_token_balance(mint.to_string(), 0);
        assert!(lm.token_wallet_snapshot_seen(mint));
    }

    #[tokio::test]
    async fn cross_dex_meteora_pump_bundle_fits_1232_with_ata_skip_and_alt() {
        let (plan, wallet, _) = build_prod_pattern_cross_dex_plan(true).await;
        let sell_ix = plan.sell_instructions.first().expect("sell leg present");
        assert_eq!(
            sell_ix.accounts.len(),
            PUMPFUN_AMM_SELL_EXTENDED_TOTAL_ACCOUNTS,
            "prod-like pump sell must use 24-account extended layout"
        );

        let (_tx, size, alt_hits, _) = compile_cross_dex_bundle_tx_optimistic_alt(&wallet, &plan);
        assert!(
            size <= MAX_SERIALIZED_TX_BYTES,
            "optimistic ALT serialized_len={size} alt_hit_count={alt_hits} (max {MAX_SERIALIZED_TX_BYTES})"
        );
    }

    /// Prod-realistic: ALT contains only `COMMON_ACCOUNTS` (EE loads on-chain ALT, no runtime merge).
    /// Documents remaining byte gap when globals are correct but pool-specific keys stay static.
    #[tokio::test]
    async fn cross_dex_meteora_pump_bundle_realistic_common_alt_size_audit() {
        let (plan, wallet, _) = build_prod_pattern_cross_dex_plan(true).await;
        let (tx, size, alt_hits, analysis) =
            compile_cross_dex_bundle_tx_realistic_alt(&wallet, &plan);

        eprintln!(
            "realistic_common_alt: serialized_len={size} alt_hit_count={alt_hits} \
             static_key_count={} alt_in_table_but_static_count={} \
             static_not_in_alt={:?} alt_in_table_but_static={:?}",
            analysis.static_key_count,
            analysis.alt_in_table_but_static_count,
            analysis
                .static_not_in_alt
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>(),
            analysis
                .alt_in_table_but_static
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>(),
        );

        // With corrected COMMON_ACCOUNTS globals, prod ALT extend should bring this under 1232.
        // Pool-specific keys (vaults, bin arrays, user ATAs) correctly remain static / not in ALT.
        assert!(
            size <= MAX_SERIALIZED_TX_BYTES,
            "realistic COMMON_ACCOUNTS ALT still too large: serialized_len={size} \
             alt_hit_count={alt_hits} static_key_count={} \
             (extend on-chain ALT per docs/ALT_GLOBAL_KEYS_AUDIT.md)",
            analysis.static_key_count
        );
        let _ = tx;
    }

    #[tokio::test]
    async fn cross_dex_meteora_pump_bundle_has_no_duplicate_static_keys() {
        let (plan, wallet, _) = build_prod_pattern_cross_dex_plan(true).await;
        let (tx, _, _, _) = compile_cross_dex_bundle_tx_realistic_alt(&wallet, &plan);
        let dupes = versioned_message_duplicate_static_keys(&tx.message);
        assert!(
            dupes.is_empty(),
            "compiled v0 message must not contain duplicate static keys (AccountLoadedTwice): {:?}",
            dupes.iter().map(|p| p.to_string()).collect::<Vec<_>>()
        );
    }

    /// Prod incident: stale `pool_accounts[1]` (`T1pyya…`) must not leak into compiled SELL ix or static keys.
    #[tokio::test]
    async fn cross_dex_pump_sell_global_config_resolved_after_wrong_pool_accounts_v14() {
        const WRONG_GLOBAL_CONFIG: &str = "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt";
        let canonical_gc = pump_amm_canonical_global_config();
        let wrong_gc = Pubkey::from_str(WRONG_GLOBAL_CONFIG).unwrap();
        assert_ne!(wrong_gc, canonical_gc);

        let wallet = Keypair::new();
        let token_mint = Pubkey::new_unique();
        let buy_pool = Pubkey::from_str("2TD1fMPg2w7Hjt8bASSdxi92YFNQFgvdznqVApe3NGpn").unwrap();
        let sell_pool = Pubkey::new_unique();

        let cache = Arc::new(LivePoolCache::new());
        seed_prod_like_cross_dex_cache(&cache, buy_pool, sell_pool, token_mint);

        // Inject prod-like wrong global_config into cache v14 row.
        if let Some(CachedPoolState::PumpAmm(state)) = cache.get(&sell_pool) {
            let mut accounts = state.pool_accounts;
            accounts[1] = wrong_gc;
            cache.set_pump_amm_pool_accounts(&sell_pool, accounts);
        }

        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let mut handler = CrossDexHandler::new(rpc.clone(), Some(wallet.pubkey()))
            .with_rpc_url("http://127.0.0.1:0".to_string())
            .with_pool_cache(cache);
        handler.init_dexes().await.expect("init_dexes");

        let mut intent = cross_dex_test_intent(&token_mint, &buy_pool, &sell_pool);
        // Intent sell_pool_accounts also carry the wrong global_config at v14[1].
        if let Some(start_idx) = intent
            .resources
            .accounts
            .iter()
            .position(|s| s.starts_with("sell_pool_accounts_start:"))
        {
            let count: usize = intent.resources.accounts[start_idx]
                .strip_prefix("sell_pool_accounts_start:")
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            let first_account_idx = start_idx + 2; // v14[1] is first_account_idx + 1
            if count >= 2 && first_account_idx < intent.resources.accounts.len() {
                intent.resources.accounts[first_account_idx] = WRONG_GLOBAL_CONFIG.to_string();
            }
        }

        let validation = cross_dex_validation_fixture(&token_mint);
        let plan = handler
            .build_swap_plan(
                &intent,
                &validation,
                CrossDexPlanOptions {
                    skip_token_ata_create: true,
                },
            )
            .await
            .expect("build_swap_plan with wrong pool_accounts[1] must succeed after normalization");

        let sell_ix = plan.sell_instructions.first().expect("sell leg");
        assert_eq!(
            sell_ix.accounts.len(),
            PUMPFUN_AMM_SELL_EXTENDED_TOTAL_ACCOUNTS,
            "prod-like pump sell must use 24-account extended layout"
        );
        assert_eq!(
            sell_ix.accounts[2].pubkey, canonical_gc,
            "SELL ix meta #2 must be canonical global_config"
        );
        assert_eq!(
            sell_ix.accounts[2].pubkey.to_string(),
            PUMPFUN_AMM_GLOBAL_CONFIG_STR
        );

        let alt_accounts = get_common_accounts();
        let (tx, _, _, _) =
            compile_cross_dex_bundle_tx_with_alt(&wallet, &plan, alt_accounts.clone());
        let dupes = versioned_message_duplicate_static_keys(&tx.message);
        assert!(
            dupes.is_empty(),
            "wrong pool_accounts[1] must not produce duplicate static keys: {:?}",
            dupes.iter().map(|p| p.to_string()).collect::<Vec<_>>()
        );

        let alt_address = match &tx.message {
            solana_sdk::message::VersionedMessage::V0(v0) => v0
                .address_table_lookups
                .first()
                .map(|l| l.account_key)
                .unwrap_or_default(),
            _ => Pubkey::default(),
        };
        let alt = LoadedAlt {
            address: alt_address,
            accounts: alt_accounts,
        };
        let loaded = loaded_addresses_from_alt_lookups(&alt, &tx.message)
            .expect("loaded addresses from ALT lookups");
        // Bundle: CU limit, CU price, Meteora buy, Pump sell, tip → sell at index 3.
        let sell_ix_index = 3;
        let resolved_gc =
            resolved_instruction_account_pubkey(&tx.message, Some(&loaded), sell_ix_index, 2)
                .expect("resolve SELL global_config from compiled v0 message");
        assert_eq!(
            resolved_gc, canonical_gc,
            "compiled v0 message must resolve SELL global_config slot to canonical pubkey"
        );

        // Wrong pubkey must not appear as a static key when normalization replaced it.
        let static_keys = match &tx.message {
            solana_sdk::message::VersionedMessage::V0(v0) => &v0.account_keys,
            solana_sdk::message::VersionedMessage::Legacy(l) => &l.account_keys,
        };
        assert!(
            !static_keys.contains(&wrong_gc),
            "wrong global_config must not be a static key in compiled bundle"
        );
    }

    #[tokio::test]
    async fn cross_dex_meteora_buy_uses_bitmap_extension_pda_when_cache_has_extension() {
        use crate::solana::dex::meteora_swap_builder::MeteoraDlmmSwapBuilder;

        let (plan, _, _) = build_prod_pattern_cross_dex_plan(true).await;
        let buy_ix = plan
            .buy_instructions
            .first()
            .expect("meteora buy ix present");
        let pool = Pubkey::from_str("2TD1fMPg2w7Hjt8bASSdxi92YFNQFgvdznqVApe3NGpn").unwrap();
        let expected =
            MeteoraDlmmSwapBuilder::derive_bitmap_extension_pda(&pool).expect("derive bitmap pda");
        assert_eq!(
            buy_ix.accounts[1].pubkey, expected,
            "pool with has_bitmap_extension=true must use bitmap-extension PDA at account index 1"
        );
        let program_id =
            Pubkey::from_str(crate::solana::dex::meteora_dlmm::METEORA_DLMM_PROGRAM).unwrap();
        assert_ne!(buy_ix.accounts[1].pubkey, program_id);
    }

    #[tokio::test]
    async fn cross_dex_ata_skip_reduces_serialized_bundle_size() {
        let (plan_with, wallet_with, _) = build_prod_pattern_cross_dex_plan(false).await;
        let (plan_without, wallet_without, _) = build_prod_pattern_cross_dex_plan(true).await;
        let (_, size_with, _) = compile_cross_dex_bundle_tx(&wallet_with, &plan_with);
        let (_, size_without, _) = compile_cross_dex_bundle_tx(&wallet_without, &plan_without);
        assert!(
            size_without < size_with,
            "ATA skip should reduce size: with={size_with} without={size_without}"
        );
    }

    #[tokio::test]
    async fn test_cross_dex_handler_pump_amm_buy_extended_cashback_builds() {
        let token_mint = Pubkey::new_unique();
        let buy_pool = Pubkey::new_unique();
        let cache = Arc::new(LivePoolCache::new());
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let buy_pool_accounts = pump_amm_v14_pool_accounts(buy_pool, token_mint);
        cache.upsert(
            buy_pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: token_mint,
                quote_mint: sol_mint,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1_000_000_000_000),
                quote_reserve: Some(50_000_000_000),
                pool_accounts: buy_pool_accounts,
                creator: None,
            }),
            100,
        );
        let third = Pubkey::new_unique();
        let tail_0 = Pubkey::new_unique();
        let tail_1 = Pubkey::new_unique();
        let fee_0 = Pubkey::from_str(PUMPFUN_AMM_BUILD_SWAP_FEE_CONFIG_STR).unwrap();
        let fee_1 = Pubkey::from_str(PUMPFUN_AMM_BUILD_SWAP_FEE_PROGRAM_STR).unwrap();
        cache.merge_pump_amm_sell_extended_layout(
            &buy_pool,
            true,
            Some(third),
            Some(tail_0),
            Some(tail_1),
            Some(fee_0),
            Some(fee_1),
            false,
            false,
            None,
        );
        cache.set_pump_amm_sell_layout_ready(&buy_pool, true);

        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let wallet = Pubkey::new_unique();
        let mut handler = CrossDexHandler::new(rpc, Some(wallet))
            .with_rpc_url("http://127.0.0.1:0".to_string())
            .with_pool_cache(cache);
        handler.init_dexes().await.expect("init_dexes");

        let ixs = handler
            .build_pump_amm_arb_swap_ixs(
                SOL_MINT,
                &token_mint.to_string(),
                100_000_000,
                1,
                &buy_pool.to_string(),
                None,
                None,
                true,
            )
            .expect("pump_amm BUY leg must build with extended cashback layout");

        assert_eq!(
            ixs[0].accounts.len(),
            PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS,
            "cashback-only BUY must use 26-account extended layout"
        );
        assert!(pump_amm_buy_program_ix_index(ixs[0].accounts.len()).is_some());
    }

    #[tokio::test]
    async fn test_cross_dex_handler_pump_amm_uses_cache() {
        let base_mint = Pubkey::new_unique();
        let pool_market = Pubkey::new_unique();
        let cache = make_pump_amm_cache_with_reserves(
            pool_market,
            base_mint,
            1_000_000_000_000,
            50_000_000_000,
        );
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let mut handler = CrossDexHandler::new(rpc.clone(), None)
            .with_rpc_url("http://127.0.0.1:0".to_string())
            .with_pool_cache(cache);
        handler.init_dexes().await.expect("init_dexes must succeed");

        let pump_amm = handler.get_dex("pump_amm").expect("pump_amm must exist");
        let router = Router::new(vec![pump_amm]);
        let base_mint_str = base_mint.to_string();
        let quote = router
            .best_quote_exact_in(SOL_MINT, &base_mint_str, 1_000_000_000)
            .await;

        assert!(quote.is_ok(), "quote should succeed");
        let route_quote = quote.unwrap();
        assert!(
            route_quote.is_some(),
            "expected Some(RouteQuote) — cache was used, otherwise RPC would have failed"
        );
    }

    #[test]
    fn test_is_cross_dex_arb_intent() {
        // Would need mock TradeIntent for proper testing
    }

    fn seed_orca_meteora_cross_dex_cache(
        cache: &LivePoolCache,
        buy_pool: Pubkey,
        sell_pool: Pubkey,
        token_mint: Pubkey,
    ) {
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        cache.upsert(
            buy_pool,
            CachedPoolState::Orca(OrcaWhirlpoolState {
                token_mint_a: token_mint,
                token_mint_b: sol_mint,
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
                tick_current_index: -624,
                sqrt_price: 1u128 << 64,
                liquidity: 5_000_000_000,
                fee_rate: 3000,
                protocol_fee_rate: 300,
                tick_spacing: 8,
                vault_a_balance: Some(1_000_000_000_000),
                vault_b_balance: Some(50_000_000_000),
                token_a_program: None,
                token_b_program: None,
            }),
            100,
        );
        cache.upsert(
            sell_pool,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: token_mint,
                token_y_mint: sol_mint,
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: -281,
                bin_step: 10,
                reserve_x_balance: Some(1_000_000_000_000),
                reserve_y_balance: Some(50_000_000_000),
            }),
            100,
        );
    }

    fn orca_meteora_cross_dex_intent(
        token_mint: &Pubkey,
        buy_pool: &Pubkey,
        sell_pool: &Pubkey,
    ) -> TradeIntent {
        let mut metadata = HashMap::new();
        metadata.insert("buy_dex".to_string(), "orca".to_string());
        metadata.insert("sell_dex".to_string(), "meteora_dlmm".to_string());
        metadata.insert("buy_pool".to_string(), buy_pool.to_string());
        metadata.insert("sell_pool".to_string(), sell_pool.to_string());
        metadata.insert("spread_bps".to_string(), "50".to_string());
        metadata.insert(
            "estimated_profit_lamports".to_string(),
            "500000".to_string(),
        );

        TradeIntent {
            header: RecordHeader {
                schema_version: 1,
                ts_unix_ms: 1,
                component: "test".to_string(),
                build: "test".to_string(),
                run_id: "run".to_string(),
            },
            intent_id: "arb-orca-meteora".to_string(),
            source: "arb-strategy".to_string(),
            tier: IntentTier::Arb,
            origin_type: IntentOrigin::StrategyA,
            deadline_slot: None,
            ttl_ms: Some(30_000),
            required_capital: ExplicitAmount {
                raw: 100_000_000,
                decimals: 9,
                ui: None,
            },
            resources: TradeResources {
                input_mint: SOL_MINT.to_string(),
                output_mint: token_mint.to_string(),
                pools: vec![buy_pool.to_string(), sell_pool.to_string()],
                accounts: vec![],
                token_program: Some(spl_token::id().to_string()),
            },
            expected_roi_bps: 50,
            max_slippage_bps: 500,
            side: TradeSide::Buy,
            regime: TradingRegime::NotApplicable,
            trigger_event_id: None,
            require_bundle: Some(true),
            bundle_tip_lamports: Some(10_000),
            hint_compute_units: None,
            hint_priority_fee_micro_lamports: None,
            hint_urgency: None,
            metadata,
            execution: None,
            swap_path: None,
        }
    }

    /// Prod pattern `orca → meteora_dlmm`: Orca buy leg must build without RPC tick-array validation.
    /// Without `set_skip_tick_array_rpc_validation(true)` in `init_dexes`, dummy RPC returns missing
    /// tick arrays → `orca tick array accounts missing` (Pre-Sim UNSUPPORTED_INTENT).
    #[tokio::test]
    async fn cross_dex_orca_buy_leg_builds_without_tick_array_rpc_validation() {
        let wallet = Keypair::new();
        let token_mint = Pubkey::new_unique();
        let buy_pool = Pubkey::new_unique();
        let sell_pool = Pubkey::new_unique();

        let cache = Arc::new(LivePoolCache::new());
        seed_orca_meteora_cross_dex_cache(&cache, buy_pool, sell_pool, token_mint);

        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let mut handler = CrossDexHandler::new(rpc, Some(wallet.pubkey())).with_pool_cache(cache);
        handler.init_dexes().await.expect("init_dexes");

        let intent = orca_meteora_cross_dex_intent(&token_mint, &buy_pool, &sell_pool);
        let validation = cross_dex_validation_fixture(&token_mint);
        let plan = handler
            .build_swap_plan(
                &intent,
                &validation,
                CrossDexPlanOptions {
                    skip_token_ata_create: true,
                },
            )
            .await
            .expect("orca buy leg must not fail tick-array RPC validation in hot path");

        assert_eq!(plan.buy_dex, "orca");
        assert_eq!(plan.sell_dex, "meteora_dlmm");
        let buy_ix = plan
            .buy_instructions
            .first()
            .expect("orca buy swap ix present");
        assert_eq!(buy_ix.program_id.to_string(), ORCA_WHIRLPOOL_PROGRAM);
        assert!(
            buy_ix.accounts.iter().any(|a| a.pubkey == buy_pool),
            "orca swap ix must reference buy whirlpool from cache injection"
        );
    }
}
