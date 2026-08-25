//! DEX Transaction/Account Parsing for MarketEvents
//!
//! Public parsing functions for market-data service to convert raw Geyser
//! updates into structured MarketEvents (PoolCreated, Trade, etc.)
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.1
//! Supports: Raydium AMM V4, Orca Whirlpool, PumpFun

use crate::solana::dex::pumpfun::{BondingCurveState, PUMPFUN_BUY_EXACT_SOL_IN_DISCRIMINATOR};
use crate::solana::dex::pumpfun_amm::{
    pump_amm_normalize_v14_pool_accounts, pump_amm_sell_extended_fields_from_ix_accounts,
    pump_amm_sell_ix_account_len_supported, pump_amm_sell_ix_discriminator,
};
use crate::solana::geyser_listener::{GeyserAccountUpdate, GeyserTransactionUpdate};
use rust_decimal::Decimal;
use solana_sdk::hash::hash;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tracing::{debug, info, trace};

// ============================================================================
// Program IDs
// ============================================================================

pub const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
pub const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
pub const METEORA_DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
pub const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const PUMPFUN_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

use std::sync::LazyLock;

/// Native SOL mint as Pubkey (for quote_mint in Trade events)
pub static SOL_MINT_PUBKEY: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str(SOL_MINT).expect("valid SOL mint"));

/// Type alias for pool lookup closure (reduces clippy::type_complexity warnings).
pub type PoolLookupFn<'a> = Option<&'a dyn Fn(&Pubkey) -> Option<OrcaPoolInfo>>;

/// Orca pool info used to deterministically parse swaps from pool state.
#[derive(Debug, Clone, Copy)]
pub struct OrcaPoolInfo {
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    pub tick_current_index: Option<i32>,
    pub tick_spacing: Option<u16>,
    pub token_a_program: Option<Pubkey>,
    pub token_b_program: Option<Pubkey>,
}

// ============================================================================
// Parsed Event Types (intermediate before conversion to MarketEventKind)
// ============================================================================

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum ParsedDexEvent {
    /// New pool created
    PoolCreated {
        pool_address: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
        dex: DexType,
        initial_liquidity_lamports: u64,
        slot: u64,
        creator: Option<Pubkey>,
        /// Optional static pool account list for deterministic intent building.
        /// Order is DEX-specific.
        pool_accounts: Option<Vec<Pubkey>>,
    },
    /// Trade/Swap observed
    Trade {
        pool_address: Pubkey,
        mint: Pubkey,
        /// Quote mint (e.g., SOL or USDC) - critical for cross-DEX price comparison
        quote_mint: Pubkey,
        trader: Pubkey,
        dex: DexType,
        is_buy: bool,
        sol_amount: u64,
        token_amount: u64,
        token_decimals: u8,
        signature: String,
        slot: u64,
        /// Optional static pool account list for deterministic intent building.
        /// Order is DEX-specific.
        pool_accounts: Option<Vec<Pubkey>>,
        /// Creator/dev wallet for PumpFun bonding curve tokens.
        /// Extracted from Fee Recipient account in swap instruction.
        creator: Option<Pubkey>,
        /// Token program for the base mint (SPL Token or Token-2022).
        /// Extracted from instruction accounts for PumpFun swaps.
        /// - SPL Token: TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
        /// - Token-2022: TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb
        token_program: Option<Pubkey>,
        /// PumpSwap only: observed `sell` used 24 accounts (three trailing cashback/volume metas).
        pump_amm_sell_requires_cashback_remaining: bool,
        /// PumpSwap only: third trailing meta from the observed ix (ix #23; not user-derivable alone).
        pump_amm_sell_cashback_third_meta: Option<Pubkey>,
        /// PumpSwap only: observed reference-trader volume meta #21 (forensics only — not used at build).
        pump_amm_sell_extended_tail_0: Option<Pubkey>,
        /// PumpSwap only: observed reference-trader volume meta #22 (forensics only — not used at build).
        pump_amm_sell_extended_tail_1: Option<Pubkey>,
        /// PumpSwap only: observed cashback `sell` fee-recipient ix #24.
        pump_amm_sell_extended_fee_tail_0: Option<Pubkey>,
        /// PumpSwap only: observed cashback `sell` fee-recipient ix #25.
        pump_amm_sell_extended_fee_tail_1: Option<Pubkey>,
        /// PumpSwap only: observed `sell` used 26 accounts (fee-recipient pair required).
        pump_amm_sell_requires_fee_tail: bool,
        /// PumpSwap only: observed `sell` used 27 accounts (pre-fee metas at ix #19/#20).
        pump_amm_sell_requires_pre_fee_metas: bool,
        /// PumpSwap only: second pre-fee meta (ix #20); first is usually global_volume_accumulator.
        pump_amm_sell_pre_fee_meta_1: Option<Pubkey>,
    },
    /// Liquidity removed (potential rug)
    LiquidityRemoved {
        pool_address: Pubkey,
        mint: Pubkey,
        sol_amount: u64,
        token_amount: u64,
        signature: String,
        slot: u64,
    },
    /// PumpFun Bonding Curve account update (contains creator)
    /// Emitted from Geyser account updates when bonding curve state changes
    BondingCurveUpdate {
        pool_address: Pubkey,
        creator: Pubkey,
        virtual_token_reserves: u64,
        virtual_sol_reserves: u64,
        real_token_reserves: u64,
        real_sol_reserves: u64,
        complete: bool,
        cashback_enabled: bool,
        slot: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DexType {
    RaydiumAmmV4,
    RaydiumCpmm,
    OrcaWhirlpool,
    MeteoraDlmm,
    PumpFun,
    PumpFunAmm,
}

impl std::fmt::Display for DexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DexType::RaydiumAmmV4 => write!(f, "raydium"),
            DexType::RaydiumCpmm => write!(f, "raydium_cpmm"),
            DexType::OrcaWhirlpool => write!(f, "orca"),
            DexType::MeteoraDlmm => write!(f, "meteora_dlmm"),
            DexType::PumpFun => write!(f, "pumpfun"),
            DexType::PumpFunAmm => write!(f, "pump_amm"),
        }
    }
}

fn anchor_disc(ix_name: &str) -> [u8; 8] {
    let out = hash(format!("global:{ix_name}").as_bytes());
    let mut disc = [0u8; 8];
    disc.copy_from_slice(&out.as_ref()[..8]);
    disc
}

// ============================================================================
// Instruction Discriminators
// ============================================================================

// PumpFun discriminators (Anchor IDL)
const PUMPFUN_CREATE: [u8; 8] = [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];
const PUMPFUN_BUY: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
const PUMPFUN_SELL: [u8; 8] = [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];

// PumpSwap create_pool discriminator (Anchor IDL: create_pool)
const PUMPFUN_AMM_CREATE_POOL: [u8; 8] = [233, 146, 209, 142, 207, 104, 64, 188];

// Raydium AMM V4 discriminators
const RAYDIUM_SWAP_BASE_IN: u8 = 9;
const RAYDIUM_SWAP_BASE_OUT: u8 = 11;
const RAYDIUM_INITIALIZE2: u8 = 1;

// Meteora DLMM swap (Anchor: sighash("global:swap"))
const METEORA_SWAP: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

// Orca Whirlpool swap (Anchor: sighash("global:swap")); program provenance disambiguates it.
const ORCA_SWAP: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

// Raydium CPMM swap (Anchor: similar pattern)
const RAYDIUM_CPMM_SWAP: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

// ============================================================================
// Main Parsing Functions (Public API)
// ============================================================================
// ============================================================================

/// Parse an account update into a DEX event (pool creation/update)
/// Used for Raydium and Orca where pool state lives in accounts
pub fn parse_account_update(update: &GeyserAccountUpdate) -> Option<ParsedDexEvent> {
    let owner_str = update.owner.to_string();

    match owner_str.as_str() {
        RAYDIUM_AMM_V4 => parse_raydium_account(update),
        ORCA_WHIRLPOOL => parse_orca_account(update),
        PUMPFUN_PROGRAM => parse_pumpfun_account(update),
        _ => None,
    }
}

/// Parse a transaction update into a DEX event (pool creation, swap)
/// Used for all DEXes for swap detection, and PumpFun for pool creation
pub fn parse_transaction_update(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    parse_transaction_update_with_pool_lookup(update, None)
}

/// Parse a transaction update into a DEX event (pool creation, swap) with optional pool lookup.
/// The pool lookup is used to resolve Orca pool mints deterministically.
///
/// Tries top-level instruction first. If that returns None and inner_instructions exist,
/// falls back to parsing CPI calls (e.g. Jupiter/aggregator-routed PumpSwap trades).
pub fn parse_transaction_update_with_pool_lookup(
    update: &GeyserTransactionUpdate,
    pool_lookup: PoolLookupFn<'_>,
) -> Option<ParsedDexEvent> {
    // 1. Try top-level instruction first
    if let Some(event) = try_parse_top_level(update, pool_lookup) {
        return Some(event);
    }

    // 2. Fallback: inner instructions (CPI from aggregators like Jupiter)
    if !update.inner_instructions.is_empty() {
        return try_parse_inner_instructions(update, pool_lookup);
    }

    None
}

/// Parse top-level instruction only (original logic).
fn try_parse_top_level(
    update: &GeyserTransactionUpdate,
    pool_lookup: PoolLookupFn<'_>,
) -> Option<ParsedDexEvent> {
    let raydium = Pubkey::from_str(RAYDIUM_AMM_V4).ok()?;
    let raydium_cpmm = Pubkey::from_str(RAYDIUM_CPMM).ok()?;
    let orca = Pubkey::from_str(ORCA_WHIRLPOOL).ok()?;
    let meteora = Pubkey::from_str(METEORA_DLMM).ok()?;
    let pumpfun = Pubkey::from_str(PUMPFUN_PROGRAM).ok()?;
    let pumpfun_amm = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).ok()?;

    let known = [meteora, raydium_cpmm, pumpfun_amm, pumpfun, raydium, orca];
    let (selected_program, parsed_update) = if update.instruction_accounts.len() >= 2
        && update.instruction_accounts[update.instruction_accounts.len() - 2]
            == crate::solana::geyser_listener::INSTRUCTION_PROGRAM_TRAILER
        && known.contains(update.instruction_accounts.last()?)
    {
        let selected = *update.instruction_accounts.last()?;
        let mut stripped = update.clone();
        stripped
            .instruction_accounts
            .truncate(stripped.instruction_accounts.len() - 2);
        (selected, Some(stripped))
    } else {
        let present: Vec<Pubkey> = known
            .into_iter()
            .filter(|program| update.account_keys.contains(program))
            .collect();
        if present.len() > 1 {
            let mut parsed = present.into_iter().filter_map(|program| match program {
                p if p == meteora => parse_meteora_transaction(update),
                p if p == raydium_cpmm => parse_raydium_cpmm_transaction(update),
                p if p == pumpfun_amm => parse_pumpfun_amm_transaction(update),
                p if p == pumpfun => parse_pumpfun_transaction(update),
                p if p == raydium => parse_raydium_transaction(update),
                p if p == orca => parse_orca_transaction(update, pool_lookup),
                _ => None,
            });
            let event = parsed.next()?;
            return parsed.next().is_none().then_some(event);
        }
        (*present.first()?, None)
    };
    let update = parsed_update.as_ref().unwrap_or(update);

    if selected_program == meteora {
        return parse_meteora_transaction(update);
    }
    if selected_program == raydium_cpmm {
        return parse_raydium_cpmm_transaction(update);
    }
    if selected_program == pumpfun_amm {
        return parse_pumpfun_amm_transaction(update);
    }
    if selected_program == pumpfun {
        return parse_pumpfun_transaction(update);
    }
    if selected_program == raydium {
        return parse_raydium_transaction(update);
    }
    if selected_program == orca {
        return parse_orca_transaction(update, pool_lookup);
    }

    None
}

/// Try to parse DEX trades from inner instructions (CPI calls).
/// Used when top-level is an aggregator (Jupiter, etc.) and the actual swap is a CPI.
fn try_parse_inner_instructions(
    update: &GeyserTransactionUpdate,
    pool_lookup: PoolLookupFn<'_>,
) -> Option<ParsedDexEvent> {
    let raydium = Pubkey::from_str(RAYDIUM_AMM_V4).ok()?;
    let raydium_cpmm = Pubkey::from_str(RAYDIUM_CPMM).ok()?;
    let orca = Pubkey::from_str(ORCA_WHIRLPOOL).ok()?;
    let meteora = Pubkey::from_str(METEORA_DLMM).ok()?;
    let pumpfun = Pubkey::from_str(PUMPFUN_PROGRAM).ok()?;
    let pumpfun_amm = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).ok()?;
    let pump_amm_sell_disc = pump_amm_sell_ix_discriminator();

    let mut pump_amm_fallback: Option<ParsedDexEvent> = None;

    for inner in &update.inner_instructions {
        if inner.data.len() < 8 {
            continue;
        }
        let Some(program_id) = update
            .account_keys
            .get(inner.program_id_index as usize)
            .copied()
        else {
            continue;
        };
        let instruction_accounts: Vec<Pubkey> = inner
            .accounts
            .iter()
            .filter_map(|idx| update.account_keys.get(*idx as usize).copied())
            .collect();

        let synth = GeyserTransactionUpdate {
            instruction_accounts,
            instruction_data: inner.data.clone(),
            inner_instructions: vec![], // avoid recursion; we only parse one level
            ..update.clone()
        };

        let result = match program_id {
            p if p == meteora => parse_meteora_transaction(&synth),
            p if p == raydium_cpmm => parse_raydium_cpmm_transaction(&synth),
            p if p == pumpfun_amm => parse_pumpfun_amm_transaction(&synth),
            p if p == pumpfun => parse_pumpfun_transaction(&synth),
            p if p == raydium => parse_raydium_transaction(&synth),
            p if p == orca => parse_orca_transaction(&synth, pool_lookup),
            _ => continue,
        };

        let Some(event) = result else {
            continue;
        };

        if program_id == pumpfun_amm {
            let is_pump_amm_sell = synth.instruction_data.get(..8) == Some(&pump_amm_sell_disc);
            if is_pump_amm_sell {
                return Some(event);
            }
            if pump_amm_fallback.is_none() {
                pump_amm_fallback = Some(event);
            }
            continue;
        }

        return Some(event);
    }

    pump_amm_fallback
}

// ============================================================================
// Raydium AMM V4 Parsing
// ============================================================================

fn parse_raydium_account(update: &GeyserAccountUpdate) -> Option<ParsedDexEvent> {
    // Raydium AMM v4 account layout: 752 bytes total
    if update.data.len() != 752 {
        return None;
    }

    let data = &update.data;

    // Offset 0: status (u64) - check if initialized
    let status = u64::from_le_bytes(data[0..8].try_into().ok()?);
    if status == 0 {
        return None; // Uninitialized
    }

    // Offset 400: coin_vault_mint (BASE MINT)
    let base_mint = Pubkey::new_from_array(data[400..432].try_into().ok()?);
    // Offset 432: pc_vault_mint (QUOTE MINT)
    let quote_mint = Pubkey::new_from_array(data[432..464].try_into().ok()?);

    // Offset 720: lp_amount (u64) - liquidity indicator
    let lp_amount = u64::from_le_bytes(data[720..728].try_into().ok()?);

    // Estimate liquidity (conservative)
    let liquidity_lamports = if lp_amount == 0 {
        5_000_000_000 // 5 SOL for new pools
    } else {
        50_000_000_000 // 50 SOL conservative
    };

    debug!(
        pool = %update.pubkey,
        base_mint = %base_mint,
        quote_mint = %quote_mint,
        "Raydium pool detected"
    );

    Some(ParsedDexEvent::PoolCreated {
        pool_address: update.pubkey,
        base_mint,
        quote_mint,
        dex: DexType::RaydiumAmmV4,
        initial_liquidity_lamports: liquidity_lamports,
        slot: update.slot,
        creator: None,
        pool_accounts: None,
    })
}

fn parse_raydium_transaction(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    if update.instruction_data.is_empty() {
        return None;
    }

    let discriminator = update.instruction_data[0];

    match discriminator {
        RAYDIUM_SWAP_BASE_IN | RAYDIUM_SWAP_BASE_OUT => {
            parse_raydium_swap(update, discriminator == RAYDIUM_SWAP_BASE_IN)
        }
        RAYDIUM_INITIALIZE2 => {
            // Pool creation via TX - we already handle via account updates
            trace!(sig = %update.signature, "Raydium initialize2 TX (handled via account)");
            None
        }
        _ => None,
    }
}

fn parse_raydium_swap(
    update: &GeyserTransactionUpdate,
    is_base_in: bool,
) -> Option<ParsedDexEvent> {
    // Raydium swap instruction accounts:
    // [0]: Token program
    // [1]: AMM ID (pool)
    // [2]: AMM authority
    // [3]: AMM open orders
    // [4]: AMM target orders
    // [5]: Pool coin token account
    // [6]: Pool pc token account
    // [7]: Serum market
    // [8]: Serum bids
    // [9]: Serum asks
    // [10]: Serum event queue
    // [11]: Serum coin vault
    // [12]: Serum pc vault
    // [13]: Serum vault signer
    // [14]: User source token
    // [15]: User destination token
    // [16]: User owner (TRADER!)

    if update.instruction_accounts.len() < 17 {
        return None;
    }

    let pool_address = update.instruction_accounts[1];
    // Use fee payer as trader for robust "unique buyers" counting.
    // In routed/aggregator swaps, the instruction "user" fields can be authorities/PDAs.
    let trader = update
        .account_keys
        .first()
        .copied()
        .unwrap_or(update.instruction_accounts[16]);

    // Parse amounts from instruction data
    // Layout: discriminator(1) + amount_in(8) + min_amount_out(8)
    if update.instruction_data.len() < 17 {
        return None;
    }

    let amount_in = u64::from_le_bytes(update.instruction_data[1..9].try_into().ok()?);
    let _min_out = u64::from_le_bytes(update.instruction_data[9..17].try_into().ok()?);

    // Determine if buy or sell based on direction
    // For SOL/token pair: base_in means selling token, base_out means buying token
    let is_buy = !is_base_in;

    // Extract mints from token balances
    let base_mint = if is_buy {
        // BUY: destination receives base tokens (account index 15)
        update
            .post_token_balances
            .iter()
            .find(|b| b.account_index == 15)
            .and_then(|b| Pubkey::from_str(&b.mint).ok())
            .unwrap_or_default()
    } else {
        // SELL: source spends base tokens (account index 14)
        update
            .pre_token_balances
            .iter()
            .find(|b| b.account_index == 14)
            .and_then(|b| Pubkey::from_str(&b.mint).ok())
            .unwrap_or_default()
    };

    // Calculate actual amounts from token balance changes
    let (sol_amount, token_amount) = if is_buy {
        // BUY: User pays SOL, receives tokens
        let tokens_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &base_mint,
        )
        .unwrap_or(0);
        (amount_in, tokens_received)
    } else {
        // SELL: User pays tokens, receives SOL (native balance, not token_balances!)
        let sol_received = calculate_native_balance_change(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &trader,
        )
        .unwrap_or(0);
        (sol_received, amount_in)
    };

    debug!(
        pool = %pool_address,
        trader = %trader,
        is_buy = is_buy,
        sol_amount = sol_amount,
        token_amount = token_amount,
        sig = %update.signature,
        "Raydium swap detected"
    );

    let token_decimals = get_token_decimals(&update.post_token_balances, &base_mint);

    // Extract actual quote_mint from token balance changes.
    // For non-SOL pairs (e.g., TOKEN/USDC), this correctly identifies the
    // quote_mint so arb-strategy can filter them out.
    let quote_mint = extract_quote_mint(&update.post_token_balances, &base_mint);

    Some(ParsedDexEvent::Trade {
        pool_address,
        mint: base_mint,
        quote_mint,
        trader,
        dex: DexType::RaydiumAmmV4,
        is_buy,
        sol_amount,
        token_amount,
        token_decimals,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: None,
        creator: None,
        token_program: None, // Raydium uses SPL Token, but we don't trade there for momentum
        pump_amm_sell_requires_cashback_remaining: false,
        pump_amm_sell_cashback_third_meta: None,
        pump_amm_sell_extended_tail_0: None,
        pump_amm_sell_extended_tail_1: None,
        pump_amm_sell_extended_fee_tail_0: None,
        pump_amm_sell_extended_fee_tail_1: None,
        pump_amm_sell_requires_fee_tail: false,
        pump_amm_sell_requires_pre_fee_metas: false,
        pump_amm_sell_pre_fee_meta_1: None,
    })
}

// ============================================================================
// Orca Whirlpool Parsing
// ============================================================================

fn parse_orca_account(update: &GeyserAccountUpdate) -> Option<ParsedDexEvent> {
    // Use existing orca_whirlpool_layout parser
    let parsed = crate::solana::dex::orca_whirlpool_layout::parse_whirlpool(&update.data)?;

    let liquidity_lamports = if parsed.liquidity > 0 {
        (parsed.liquidity / 1_000_000).min(1_000_000_000_000) as u64
    } else {
        5_000_000_000
    };

    debug!(
        pool = %update.pubkey,
        base_mint = %parsed.token_mint_a,
        quote_mint = %parsed.token_mint_b,
        "Orca pool detected"
    );

    Some(ParsedDexEvent::PoolCreated {
        pool_address: update.pubkey,
        base_mint: parsed.token_mint_a,
        quote_mint: parsed.token_mint_b,
        dex: DexType::OrcaWhirlpool,
        initial_liquidity_lamports: liquidity_lamports,
        slot: update.slot,
        creator: None,
        pool_accounts: None,
    })
}

fn parse_orca_transaction(
    update: &GeyserTransactionUpdate,
    pool_lookup: PoolLookupFn<'_>,
) -> Option<ParsedDexEvent> {
    // Orca Whirlpool swap detection
    // Discriminator for swap: sighash("global:swap") = first 8 bytes
    if update.instruction_data.len() < 8 {
        return None;
    }

    let disc = &update.instruction_data[0..8];
    if disc != ORCA_SWAP {
        return None;
    }

    // Orca swap accounts:
    // [0]: Token program
    // [1]: Token authority
    // [2]: Whirlpool
    // [3]: Token owner (TRADER!)
    // ...
    if update.instruction_accounts.len() < 4 {
        return None;
    }

    let pool_address = update.instruction_accounts[2];
    let trader = update
        .account_keys
        .first()
        .copied()
        .unwrap_or(update.instruction_accounts[3]);

    // Parse amounts
    // Layout: discriminator(8) + amount(8) + other_amount_threshold(8) + sqrt_price_limit(16) +
    //         amount_specified_is_input(1) + a_to_b(1)
    if update.instruction_data.len() < 42 {
        return None;
    }

    let amount = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let amount_specified_is_input = update.instruction_data[40] == 1;
    let a_to_b = update.instruction_data[41] == 1; // direction flag
    let pool_info = pool_lookup.and_then(|f| f(&pool_address));
    let sol_mint = *SOL_MINT_PUBKEY;

    let (base_mint, quote_mint, is_buy, token_program, pool_accounts) =
        if let Some(info) = pool_info {
            let (base_mint, quote_mint, is_buy) =
                if info.token_mint_a == sol_mint && info.token_mint_b != sol_mint {
                    (info.token_mint_b, sol_mint, a_to_b)
                } else if info.token_mint_b == sol_mint && info.token_mint_a != sol_mint {
                    (info.token_mint_a, sol_mint, !a_to_b)
                } else {
                    (info.token_mint_a, info.token_mint_b, !a_to_b)
                };

            let token_program = if base_mint == info.token_mint_a {
                info.token_a_program
            } else if base_mint == info.token_mint_b {
                info.token_b_program
            } else {
                None
            };

            let mut accounts = vec![
                pool_address,
                info.token_mint_a,
                info.token_mint_b,
                info.token_vault_a,
                info.token_vault_b,
            ];
            accounts.retain(|pk| pk != &Pubkey::default());

            (base_mint, quote_mint, is_buy, token_program, Some(accounts))
        } else {
            // Extract mints from token balances (fallback heuristic)
            let base_mint = if !a_to_b {
                // BUY: Find token account with increasing balance
                update
                    .post_token_balances
                    .iter()
                    .find(|post| {
                        let post_amt: u64 = post.ui_token_amount.amount.parse().unwrap_or(0);
                        let pre_amt: u64 = update
                            .pre_token_balances
                            .iter()
                            .find(|pre| {
                                pre.mint == post.mint && pre.account_index == post.account_index
                            })
                            .and_then(|pre| pre.ui_token_amount.amount.parse().ok())
                            .unwrap_or(0);
                        post_amt > pre_amt && post.mint != SOL_MINT
                    })
                    .and_then(|b| Pubkey::from_str(&b.mint).ok())
                    .unwrap_or_default()
            } else {
                // SELL: Find token account with decreasing balance
                update
                    .pre_token_balances
                    .iter()
                    .find(|pre| {
                        let pre_amt: u64 = pre.ui_token_amount.amount.parse().unwrap_or(0);
                        let post_amt: u64 = update
                            .post_token_balances
                            .iter()
                            .find(|post| {
                                post.mint == pre.mint && post.account_index == pre.account_index
                            })
                            .and_then(|post| post.ui_token_amount.amount.parse().ok())
                            .unwrap_or(0);
                        pre_amt > post_amt && pre.mint != SOL_MINT
                    })
                    .and_then(|b| Pubkey::from_str(&b.mint).ok())
                    .unwrap_or_default()
            };
            // Fallback: Extract token_program from post_token_balances
            let spl_token_str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
            let token_2022_str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
            let fallback_token_program = update
                .post_token_balances
                .iter()
                .find(|b| Pubkey::from_str(&b.mint).ok() == Some(base_mint))
                .and_then(|b| b.program_id.as_ref())
                .and_then(|p| Pubkey::from_str(p).ok())
                .filter(|tp| {
                    let tp_str = tp.to_string();
                    tp_str == spl_token_str || tp_str == token_2022_str
                });
            (base_mint, sol_mint, !a_to_b, fallback_token_program, None)
        };

    // Calculate actual amounts from token balance changes
    let tokens_changed = calculate_token_balance_change(
        &update.pre_token_balances,
        &update.post_token_balances,
        &base_mint,
    )
    .unwrap_or(0);
    let sol_changed = if quote_mint == sol_mint {
        calculate_native_balance_change(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &trader,
        )
        .unwrap_or(0)
    } else {
        0
    };
    let token_amount = if tokens_changed > 0 {
        tokens_changed
    } else {
        amount
    };
    let sol_amount = if sol_changed > 0 {
        sol_changed
    } else if quote_mint == sol_mint && amount_specified_is_input {
        amount
    } else {
        0
    };

    debug!(
        pool = %pool_address,
        trader = %trader,
        is_buy = is_buy,
        sol_amount = sol_amount,
        token_amount = token_amount,
        sig = %update.signature,
        "Orca swap detected"
    );

    let token_decimals = get_token_decimals(&update.post_token_balances, &base_mint);

    Some(ParsedDexEvent::Trade {
        pool_address,
        mint: base_mint,
        quote_mint,
        trader,
        dex: DexType::OrcaWhirlpool,
        is_buy,
        sol_amount,
        token_amount,
        token_decimals,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts,
        creator: None,
        token_program,
        pump_amm_sell_requires_cashback_remaining: false,
        pump_amm_sell_cashback_third_meta: None,
        pump_amm_sell_extended_tail_0: None,
        pump_amm_sell_extended_tail_1: None,
        pump_amm_sell_extended_fee_tail_0: None,
        pump_amm_sell_extended_fee_tail_1: None,
        pump_amm_sell_requires_fee_tail: false,
        pump_amm_sell_requires_pre_fee_metas: false,
        pump_amm_sell_pre_fee_meta_1: None,
    })
}

// ============================================================================
// PumpFun Parsing
// ============================================================================

/// Parse PumpFun bonding curve account update.
/// Extracts creator from account data (bytes 49-80).
/// This enables creator discovery even when we miss the CREATE transaction.
fn parse_pumpfun_account(update: &GeyserAccountUpdate) -> Option<ParsedDexEvent> {
    // Bonding curve accounts are 81+ bytes
    if update.data.len() < 81 {
        return None;
    }

    // Parse using existing BondingCurveState parser
    let state = BondingCurveState::parse(&update.data).ok()?;

    // Only emit for non-completed curves (active trading)
    // Completed curves have migrated to Raydium
    if state.complete {
        trace!(
            pool = %update.pubkey,
            "PumpFun bonding curve completed (migrated to Raydium), skipping"
        );
        return None;
    }

    debug!(
        pool = %update.pubkey,
        creator = %state.creator,
        virtual_sol = state.virtual_sol_reserves,
        virtual_token = state.virtual_token_reserves,
        slot = update.slot,
        "PumpFun BondingCurveUpdate parsed from account"
    );

    Some(ParsedDexEvent::BondingCurveUpdate {
        pool_address: update.pubkey,
        creator: state.creator,
        virtual_token_reserves: state.virtual_token_reserves,
        virtual_sol_reserves: state.virtual_sol_reserves,
        real_token_reserves: state.real_token_reserves,
        real_sol_reserves: state.real_sol_reserves,
        complete: state.complete,
        cashback_enabled: state.cashback_enabled,
        slot: update.slot,
    })
}

fn parse_pumpfun_transaction(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    if update.instruction_data.len() < 8 {
        return None;
    }

    let disc = &update.instruction_data[0..8];

    if disc == PUMPFUN_CREATE {
        return parse_pumpfun_create(update);
    }
    if disc == PUMPFUN_BUY_EXACT_SOL_IN_DISCRIMINATOR {
        return parse_pumpfun_buy_exact_sol_in(update);
    }
    if disc == PUMPFUN_BUY {
        return parse_pumpfun_swap(update, true);
    }
    if disc == PUMPFUN_SELL {
        return parse_pumpfun_swap(update, false);
    }

    None
}

fn parse_pumpfun_create(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    // PumpFun CREATE instruction accounts:
    // [0]: Mint (the token being created!)
    // [1]: Mint Authority
    // [2]: Bonding Curve
    // [3]: Associated Bonding Curve (vault)
    // [4]: Global
    // [5]: Metaplex Token Metadata Program
    // [6]: Metadata account
    // [7]: User (creator, fee payer)

    // DEBUG: Log account count before returning None
    if update.instruction_accounts.len() < 8 {
        info!(
            sig = %update.signature,
            slot = update.slot,
            account_count = update.instruction_accounts.len(),
            "⚠️  PumpFun CREATE: insufficient accounts (need 8)"
        );
        return None;
    }

    let token_mint = update.instruction_accounts[0];
    let creator = update.instruction_accounts[7];

    // Calculate Bonding Curve PDA
    let pumpfun_program = Pubkey::from_str(PUMPFUN_PROGRAM).ok()?;
    let (bonding_curve, _) =
        Pubkey::find_program_address(&[b"bonding-curve", token_mint.as_ref()], &pumpfun_program);

    let quote_mint = Pubkey::from_str(SOL_MINT).ok()?;

    info!(
        sig = %update.signature,
        slot = update.slot,
        mint = %token_mint,
        bonding_curve = %bonding_curve,
        creator = %creator,
        "🆕 PumpFun CREATE detected"
    );

    Some(ParsedDexEvent::PoolCreated {
        pool_address: bonding_curve,
        base_mint: token_mint,
        quote_mint,
        dex: DexType::PumpFun,
        initial_liquidity_lamports: 30_000_000_000, // 30 SOL default
        slot: update.slot,
        creator: Some(creator),
        pool_accounts: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_pumpfun_trade_event(
    update: &GeyserTransactionUpdate,
    bonding_curve: Pubkey,
    mint: Pubkey,
    trader: Pubkey,
    is_buy: bool,
    sol_amount: u64,
    token_amount: u64,
    creator: Option<Pubkey>,
) -> ParsedDexEvent {
    let token_decimals = get_token_decimals(&update.post_token_balances, &mint);

    let spl_token_str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    let token_2022_str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

    let token_program = update
        .post_token_balances
        .iter()
        .find(|b| Pubkey::from_str(&b.mint).ok() == Some(mint))
        .and_then(|b| b.program_id.as_ref())
        .and_then(|p| Pubkey::from_str(p).ok())
        .filter(|tp| {
            let tp_str = tp.to_string();
            tp_str == spl_token_str || tp_str == token_2022_str
        });

    if let Some(ref tp) = token_program {
        debug!(
            mint = %mint,
            token_program = %tp,
            sig = %update.signature,
            "PumpFun swap: extracted token_program from post_token_balances"
        );
    } else {
        debug!(
            mint = %mint,
            sig = %update.signature,
            "PumpFun swap: no token_program in post_token_balances (fallback to None)"
        );
    }

    debug!(
        pool = %bonding_curve,
        mint = %mint,
        trader = %trader,
        is_buy = is_buy,
        sol_amount = sol_amount,
        token_amount = token_amount,
        sig = %update.signature,
        "PumpFun swap detected"
    );

    ParsedDexEvent::Trade {
        pool_address: bonding_curve,
        mint,
        quote_mint: *SOL_MINT_PUBKEY,
        trader,
        dex: DexType::PumpFun,
        is_buy,
        sol_amount,
        token_amount,
        token_decimals,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: None,
        creator,
        token_program,
        pump_amm_sell_requires_cashback_remaining: false,
        pump_amm_sell_cashback_third_meta: None,
        pump_amm_sell_extended_tail_0: None,
        pump_amm_sell_extended_tail_1: None,
        pump_amm_sell_extended_fee_tail_0: None,
        pump_amm_sell_extended_fee_tail_1: None,
        pump_amm_sell_requires_fee_tail: false,
        pump_amm_sell_requires_pre_fee_metas: false,
        pump_amm_sell_pre_fee_meta_1: None,
    }
}

fn parse_pumpfun_buy_exact_sol_in(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    if update.instruction_accounts.len() < 7 {
        return None;
    }
    if update.instruction_data.len() < 24 {
        return None;
    }

    let mint = update.instruction_accounts[2];
    let bonding_curve = update.instruction_accounts[3];
    let trader = update
        .account_keys
        .first()
        .copied()
        .unwrap_or(update.instruction_accounts[6]);

    let swap_sol_used = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let wallet_gross_delta = calculate_native_balance_change(
        &update.account_keys,
        &update.pre_balances,
        &update.post_balances,
        &trader,
    )
    .unwrap_or(0);

    let tokens_received = calculate_token_balance_change(
        &update.pre_token_balances,
        &update.post_token_balances,
        &mint,
    )
    .unwrap_or(0);

    if tokens_received == 0 {
        trace!(
            sig = %update.signature,
            "PumpFun buy_exact_sol_in: zero token delta; skip"
        );
        return None;
    }

    let token_decimals = get_token_decimals(&update.post_token_balances, &mint);
    let token_ui = tokens_received as f64 / 10f64.powi(i32::from(token_decimals));
    let sol_ui = swap_sol_used as f64 / 1e9;
    let approx_tps = if sol_ui > f64::EPSILON {
        token_ui / sol_ui
    } else {
        0.0
    };

    trace!(
        sig = %update.signature,
        wallet_gross_delta,
        swap_sol_used,
        tokens = tokens_received,
        approx_tps,
        "PumpFun buy_exact_sol_in parsed (swap SOL from ix)"
    );

    Some(build_pumpfun_trade_event(
        update,
        bonding_curve,
        mint,
        trader,
        true,
        swap_sol_used,
        tokens_received,
        None,
    ))
}

fn parse_pumpfun_swap(update: &GeyserTransactionUpdate, is_buy: bool) -> Option<ParsedDexEvent> {
    // PumpFun BUY/SELL instruction accounts:
    // [0]: Global
    // [1]: Fee Recipient
    // [2]: Mint
    // [3]: Bonding Curve
    // [4]: Associated Bonding Curve
    // [5]: Associated User (user's token account)
    // [6]: User (TRADER!)
    // [7]: System Program
    // [8]: Token Program
    // [9]: Rent (optional)
    // [10]: Event Authority
    // [11]: Program

    if update.instruction_accounts.len() < 7 {
        return None;
    }

    let mint = update.instruction_accounts[2];
    let bonding_curve = update.instruction_accounts[3];
    let trader = update
        .account_keys
        .first()
        .copied()
        .unwrap_or(update.instruction_accounts[6]);

    // Parse amounts from instruction data
    // BUY layout: discriminator(8) + token_amount(8) + max_sol_cost(8)
    // SELL layout: discriminator(8) + token_amount(8) + min_sol_output(8)
    if update.instruction_data.len() < 24 {
        return None;
    }

    let token_amount_param = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let _sol_limit = u64::from_le_bytes(update.instruction_data[16..24].try_into().ok()?);

    // Note: quote_mint not needed - PumpFun BC uses native balance for SOL
    let _quote_mint =
        Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap_or_default();

    let (sol_amount, token_amount) = if is_buy {
        let wallet_gross_delta = calculate_native_balance_change(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &trader,
        )
        .unwrap_or(0);

        let curve_delta = native_lamports_signed_delta(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &bonding_curve,
        );

        let swap_sol_used = curve_delta
            .filter(|&d| d > 0)
            .and_then(|d| u64::try_from(d).ok());

        let Some(swap_sol_used) = swap_sol_used else {
            trace!(
                sig = %update.signature,
                ?curve_delta,
                wallet_gross_delta,
                bonding_curve = %bonding_curve,
                "PumpFun legacy BUY: bonding curve native inflow missing or non-positive; skip trade (max_sol_cost not used for price)"
            );
            return None;
        };

        let tokens_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &mint,
        )
        .unwrap_or(0);

        if tokens_received == 0 {
            trace!(
                sig = %update.signature,
                "PumpFun legacy BUY: zero token delta; skip"
            );
            return None;
        }

        let token_decimals = get_token_decimals(&update.post_token_balances, &mint);
        let token_ui = tokens_received as f64 / 10f64.powi(i32::from(token_decimals));
        let sol_ui = swap_sol_used as f64 / 1e9;
        let approx_tps = if sol_ui > f64::EPSILON {
            token_ui / sol_ui
        } else {
            0.0
        };

        trace!(
            sig = %update.signature,
            wallet_gross_delta,
            swap_sol_used,
            tokens = tokens_received,
            approx_tps,
            "PumpFun legacy BUY parsed (swap SOL = bonding curve inflow)"
        );

        (swap_sol_used, tokens_received)
    } else {
        let curve_delta = native_lamports_signed_delta(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &bonding_curve,
        );

        let swap_sol_out = curve_delta
            .filter(|&d| d < 0)
            .and_then(|d| u64::try_from(-d).ok());

        let wallet_sol_delta = calculate_native_balance_change(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &trader,
        )
        .unwrap_or(0);

        let sol_to_trader = swap_sol_out.unwrap_or_else(|| {
            trace!(
                sig = %update.signature,
                ?curve_delta,
                wallet_sol_delta,
                "PumpFun SELL: curve outflow unavailable; using trader wallet native delta"
            );
            wallet_sol_delta
        });

        (sol_to_trader, token_amount_param)
    };

    Some(build_pumpfun_trade_event(
        update,
        bonding_curve,
        mint,
        trader,
        is_buy,
        sol_amount,
        token_amount,
        None,
    ))
}

// ============================================================================
// Pump.fun AMM (PumpSwap) Parsing
// ============================================================================

/// Try to parse create_pool from top-level or inner instructions.
///
/// Does **not** attach synthetic `pool_accounts` — create_pool lacks swap-instruction metas (fee
/// recipients, volume accumulators, etc.); usable accounts come from verified swap trades or
/// cold-path discovery (`EnsurePumpAmmPoolAccounts`).
fn try_parse_pumpfun_amm_create_pool(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    let pumpfun_amm = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).ok()?;

    // 1. Check top-level instruction
    if update.instruction_data.len() >= 8
        && update.instruction_data[0..8] == PUMPFUN_AMM_CREATE_POOL
        && update.instruction_accounts.len() >= 18
    {
        let pool = update.instruction_accounts[0];
        let base_mint = update.instruction_accounts[3];
        let quote_mint = update.instruction_accounts[4];
        let creator = update.instruction_accounts[2];
        info!(
            pool = %pool,
            base_mint = %base_mint,
            "PumpSwap create_pool detected (top-level) — observation only (no synthetic pool_accounts)"
        );
        return Some(ParsedDexEvent::PoolCreated {
            pool_address: pool,
            base_mint,
            quote_mint,
            dex: DexType::PumpFunAmm,
            initial_liquidity_lamports: 0,
            slot: update.slot,
            creator: Some(creator),
            pool_accounts: None,
        });
    }

    // 2. Check inner instructions (e.g. migrate CPI)
    for inner in &update.inner_instructions {
        if inner.data.len() < 8 {
            continue;
        }
        if inner.data[0..8] != PUMPFUN_AMM_CREATE_POOL {
            continue;
        }
        let program_id = update
            .account_keys
            .get(inner.program_id_index as usize)
            .copied()?;
        if program_id != pumpfun_amm {
            continue;
        }
        let accounts: Vec<Pubkey> = inner
            .accounts
            .iter()
            .filter_map(|&idx| update.account_keys.get(idx as usize).copied())
            .collect();
        if accounts.len() < 18 {
            continue;
        }
        let pool = accounts[0];
        let base_mint = accounts[3];
        let quote_mint = accounts[4];
        let creator = accounts[2];
        info!(
            pool = %pool,
            base_mint = %base_mint,
            "PumpSwap create_pool detected (inner/CPI) — observation only (no synthetic pool_accounts)"
        );
        return Some(ParsedDexEvent::PoolCreated {
            pool_address: pool,
            base_mint,
            quote_mint,
            dex: DexType::PumpFunAmm,
            initial_liquidity_lamports: 0,
            slot: update.slot,
            creator: Some(creator),
            pool_accounts: None,
        });
    }

    None
}

fn parse_pumpfun_amm_transaction(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    // Try create_pool first (observation-only; no synthetic pool_accounts)
    if let Some(created) = try_parse_pumpfun_amm_create_pool(update) {
        return Some(created);
    }

    if update.instruction_data.len() < 24 {
        return None;
    }
    let buy_disc = anchor_disc("buy_exact_quote_in");
    let sell_disc = pump_amm_sell_ix_discriminator();
    let disc: [u8; 8] = update.instruction_data[0..8].try_into().ok()?;

    let is_buy = if disc == buy_disc {
        if update.instruction_accounts.len() < 23 {
            return None;
        }
        true
    } else if disc == sell_disc {
        if !pump_amm_sell_ix_account_len_supported(update.instruction_accounts.len()) {
            return None;
        }
        false
    } else {
        return None;
    };

    let sell_ext = if is_buy {
        None
    } else {
        pump_amm_sell_extended_fields_from_ix_accounts(&update.instruction_accounts)
    };
    let sell_requires_cashback_remaining = sell_ext.map(|e| e.requires_extended).unwrap_or(false);
    let sell_requires_fee_tail = sell_ext.map(|e| e.requires_fee_tail).unwrap_or(false);
    let sell_requires_pre_fee_metas = sell_ext.map(|e| e.requires_pre_fee_metas).unwrap_or(false);
    let sell_cashback_third_meta = sell_ext.and_then(|e| e.third_meta);
    let sell_extended_tail_0 = sell_ext.and_then(|e| e.tail_0);
    let sell_extended_tail_1 = sell_ext.and_then(|e| e.tail_1);
    let sell_extended_fee_tail_0 = sell_ext.and_then(|e| e.fee_tail_0);
    let sell_extended_fee_tail_1 = sell_ext.and_then(|e| e.fee_tail_1);
    let sell_pre_fee_meta_1 = sell_ext.and_then(|e| e.pre_fee_meta_1);
    if sell_requires_cashback_remaining && sell_cashback_third_meta.is_none() {
        return None;
    }
    if sell_ext.is_some_and(|e| e.requires_fee_tail)
        && (sell_extended_fee_tail_0.is_none() || sell_extended_fee_tail_1.is_none())
    {
        return None;
    }
    if sell_ext.is_some_and(|e| e.requires_pre_fee_metas) && sell_pre_fee_meta_1.is_none() {
        return None;
    }

    // Observed account order from on-chain PumpFun AMM swap TX:
    // BUY: 23 accounts (includes global_volume_accumulator + user_volume)
    // SELL: 21 base; 24 = legacy extended (#21/#22 volume, #23 pool-v2) OR post-upgrade non-cashback (#21 pool-v2);
    //       26 = cashback extended (#21/#22 volume, #23 pool-v2, #24/#25 fee recipient pair).
    // See pumpfun_amm.rs `pump_amm_sell_extended_fields_from_ix_accounts` and build_swap_ix_from_pool_accounts.
    let pool_market = update.instruction_accounts[0];
    let trader = update
        .account_keys
        .first()
        .copied()
        .unwrap_or(update.instruction_accounts[1]);
    let global_config = update.instruction_accounts[2];
    let base_mint = update.instruction_accounts[3];
    // instruction_accounts[5] = user_base_ta
    // instruction_accounts[6] = user_quote_ta
    let quote_mint = update
        .instruction_accounts
        .get(4)
        .copied()
        .unwrap_or(*SOL_MINT_PUBKEY);
    let pool_base_vault = update.instruction_accounts[7];
    let pool_quote_vault = update.instruction_accounts[8];
    let protocol_fee_recipient = update.instruction_accounts[9];
    let protocol_fee_recipient_ta = update.instruction_accounts[10];
    // instruction_accounts[11..14] = spl_token, spl_token, system, ata_program (readonly)
    let event_authority = update.instruction_accounts[15];

    // BUY vs SELL differ after account 15:
    // BUY: [16]=global_volume_accumulator, [17]=coin_creator_vault_ata, [18]=coin_creator_vault_authority,
    //      [19]=user_volume, [20]=fee_config (pool-specific PDA), [21]=fee_program, [22]=program_id
    // SELL: [16]=program_id, [17]=coin_creator_vault_ata, [18]=coin_creator_vault_authority,
    //       [19]=fee_config global, [20]=fee_program;
    //       extended: #21/#22 volume metas, #23 pool-v2 (26-account also has #24/#25 fee pair).
    //
    // CRITICAL: BUY uses pool-specific fee_config in the ix; we still publish global constants in v14 cache.
    //   - PUMPFUN_AMM_FEE_CONFIG = 5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx
    //   - PUMPFUN_AMM_FEE_PROGRAM_ID = pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ
    let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).ok()?;
    let global_volume_accumulator = if is_buy {
        update.instruction_accounts[16]
    } else {
        Pubkey::find_program_address(&[b"global_volume_accumulator"], &pump_amm_program).0
    };
    let coin_creator_vault_ata = update.instruction_accounts[17];
    let coin_creator_vault_authority = update.instruction_accounts[18];

    // Use global fee constants (NOT transaction accounts which are pool-specific PDAs)
    let fee_config = Pubkey::from_str("5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx")
        .expect("hardcoded PUMPFUN_AMM_FEE_CONFIG");
    let fee_program = Pubkey::from_str("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")
        .expect("hardcoded PUMPFUN_AMM_FEE_PROGRAM_ID");

    let amount_in = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let _min_out = u64::from_le_bytes(update.instruction_data[16..24].try_into().ok()?);

    // Calculate actual amount_out from token balance changes
    // For BUY: user receives base tokens (token_amount), SOL from native balance
    // For SELL: user receives WSOL as native balance, tokens from token balance
    let (sol_amount, token_amount) = if is_buy {
        let tokens_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &base_mint,
        )
        .unwrap_or(0);

        let wallet_gross_delta = calculate_native_balance_change(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &trader,
        )
        .unwrap_or(0);

        const PUMP_AMM_BUY_WALLET_SOL_WARN_LAMPORTS: u64 = 50_000;
        if wallet_gross_delta.abs_diff(amount_in) > PUMP_AMM_BUY_WALLET_SOL_WARN_LAMPORTS {
            trace!(
                sig = %update.signature,
                wallet_gross_delta,
                swap_sol_used = amount_in,
                tokens = tokens_received,
                "Pump.fun AMM BUY: wallet native delta differs from ix amount_in (fees/tips); using amount_in for price"
            );
        }

        (amount_in, tokens_received)
    } else {
        // SELL: amount_in is tokens, need to calculate WSOL received from native balance
        let sol_received = calculate_native_balance_change(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &trader,
        )
        .unwrap_or(0);

        (sol_received, amount_in)
    };

    // Pool static accounts in v1 format (14 accounts, includes global_volume_accumulator)
    // See docs/MOMENTUM_V2_SPEC.md section 9.2 and pumpfun_amm.rs build_swap_ix_from_pool_accounts
    let pool_accounts = {
        let mut accounts = vec![
            pool_market,                  // [0]
            global_config,                // [1]
            base_mint,                    // [2]
            quote_mint,                   // [3]
            pool_base_vault,              // [4]
            pool_quote_vault,             // [5]
            protocol_fee_recipient,       // [6]
            protocol_fee_recipient_ta,    // [7]
            event_authority,              // [8]
            coin_creator_vault_ata,       // [9]
            coin_creator_vault_authority, // [10]
            global_volume_accumulator,    // [11]
            fee_config,                   // [12]
            fee_program,                  // [13]
        ];
        pump_amm_normalize_v14_pool_accounts(&pool_market, &mut accounts);
        accounts
    };

    debug!(
        pool = %pool_market,
        mint = %base_mint,
        trader = %trader,
        is_buy = is_buy,
        amount_in = amount_in,
        sol_amount = sol_amount,
        token_amount = token_amount,
        sig = %update.signature,
        "Pump.fun AMM swap detected"
    );

    let token_decimals = get_token_decimals(&update.post_token_balances, &base_mint);

    // Extract Token Program from post_token_balances (authoritative source)
    let spl_token_str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    let token_2022_str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

    let token_program = update
        .post_token_balances
        .iter()
        .find(|b| Pubkey::from_str(&b.mint).ok() == Some(base_mint))
        .and_then(|b| b.program_id.as_ref())
        .and_then(|p| Pubkey::from_str(p).ok())
        .filter(|tp| {
            let tp_str = tp.to_string();
            tp_str == spl_token_str || tp_str == token_2022_str
        });

    Some(ParsedDexEvent::Trade {
        pool_address: pool_market,
        mint: base_mint,
        quote_mint: *SOL_MINT_PUBKEY,
        trader,
        dex: DexType::PumpFunAmm,
        is_buy,
        sol_amount,
        token_amount,
        token_decimals,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: Some(pool_accounts),
        creator: None, // PumpSwap uses coin_creator_vault_authority from pool_accounts[10]
        token_program,
        pump_amm_sell_requires_cashback_remaining: sell_requires_cashback_remaining,
        pump_amm_sell_cashback_third_meta: sell_cashback_third_meta,
        pump_amm_sell_extended_tail_0: sell_extended_tail_0,
        pump_amm_sell_extended_tail_1: sell_extended_tail_1,
        pump_amm_sell_extended_fee_tail_0: sell_extended_fee_tail_0,
        pump_amm_sell_extended_fee_tail_1: sell_extended_fee_tail_1,
        pump_amm_sell_requires_fee_tail: sell_requires_fee_tail,
        pump_amm_sell_requires_pre_fee_metas: sell_requires_pre_fee_metas,
        pump_amm_sell_pre_fee_meta_1: sell_pre_fee_meta_1,
    })
}

/// Extract the quote_mint from token balance records by finding the mint that
/// is different from `base_mint`. In a swap, exactly 2 mints are involved:
/// the base token and the quote token. This returns the one that isn't `base_mint`.
///
/// Falls back to SOL_MINT_PUBKEY if no other mint is found (e.g., if balances
/// are missing or the transaction only involves a single mint).
fn extract_quote_mint(
    balances: &[crate::solana::geyser_listener::TokenBalance],
    base_mint: &Pubkey,
) -> Pubkey {
    balances
        .iter()
        .filter_map(|b| Pubkey::from_str(&b.mint).ok())
        .find(|m| m != base_mint)
        .unwrap_or(*SOL_MINT_PUBKEY)
}

/// Calculate token balance change for a specific mint
/// Returns positive value for increase (received), negative for decrease (sent)
fn calculate_token_balance_change(
    pre_balances: &[crate::solana::geyser_listener::TokenBalance],
    post_balances: &[crate::solana::geyser_listener::TokenBalance],
    mint: &Pubkey,
) -> Option<u64> {
    let mint_str = mint.to_string();

    // For Jupiter/aggregator trades, there may be multiple token accounts for the same mint
    // (e.g., trader's ATA + pool's token vault). We need to find the one that actually changed.
    // Strategy: Find the account with the largest absolute balance change

    let mut max_change: u64 = 0;

    for post in post_balances.iter().filter(|b| b.mint == mint_str) {
        let post_amount = post.ui_token_amount.amount.parse::<u64>().ok()?;

        // Find matching pre balance by account_index
        if let Some(pre) = pre_balances
            .iter()
            .find(|b| b.account_index == post.account_index)
        {
            let pre_amount = pre.ui_token_amount.amount.parse::<u64>().ok()?;

            // Calculate absolute change
            let change = post_amount.abs_diff(pre_amount);

            if change > max_change {
                max_change = change;
            }
        }
    }

    if max_change > 0 {
        Some(max_change)
    } else {
        None
    }
}

/// Calculate native SOL balance change for a specific account
/// WSOL is tracked as native lamports in balances array, not token_balances!
/// Returns absolute change in lamports
fn calculate_native_balance_change(
    account_keys: &[Pubkey],
    pre_balances: &[u64],
    post_balances: &[u64],
    account: &Pubkey,
) -> Option<u64> {
    // Find account index in account_keys
    let account_index = account_keys.iter().position(|k| k == account)?;

    // Get pre and post native balances (lamports)
    let pre = *pre_balances.get(account_index)?;
    let post = *post_balances.get(account_index)?;

    // Return absolute difference
    // For SELL: post > pre (user receives SOL)
    // For BUY: pre > post (user spends SOL)
    Some(post.abs_diff(pre))
}

fn native_lamports_signed_delta(
    account_keys: &[Pubkey],
    pre_balances: &[u64],
    post_balances: &[u64],
    account: &Pubkey,
) -> Option<i128> {
    let account_index = account_keys.iter().position(|k| k == account)?;
    let pre = *pre_balances.get(account_index)? as i128;
    let post = *post_balances.get(account_index)? as i128;
    Some(post - pre)
}

/// Extract token decimals from token_balances
/// Returns decimals if found, otherwise defaults to 9 (most Solana tokens)
fn get_token_decimals(
    post_balances: &[crate::solana::geyser_listener::TokenBalance],
    mint: &Pubkey,
) -> u8 {
    let mint_str = mint.to_string();
    post_balances
        .iter()
        .find(|b| b.mint == mint_str)
        .map(|b| b.ui_token_amount.decimals)
        .unwrap_or(9) // Default to 9 decimals (most pump.fun and Solana tokens)
}

// ============================================================================
// Helper to convert ParsedDexEvent to MarketEventKind
// ============================================================================

use crate::ipc::MarketEventKind;

impl ParsedDexEvent {
    /// Convert to MarketEventKind for NATS/JSONL output
    pub fn to_market_event_kind(&self) -> MarketEventKind {
        match self {
            ParsedDexEvent::PoolCreated {
                pool_address,
                base_mint,
                quote_mint,
                dex,
                initial_liquidity_lamports,
                ..
            } => MarketEventKind::PoolCreated {
                pool_address: pool_address.to_string(),
                base_mint: base_mint.to_string(),
                quote_mint: quote_mint.to_string(),
                dex: dex.to_string(),
                initial_liquidity_sol: if *initial_liquidity_lamports > 0 {
                    Some(
                        Decimal::from(*initial_liquidity_lamports)
                            / Decimal::from(1_000_000_000u64),
                    )
                } else {
                    None
                },
            },
            ParsedDexEvent::Trade {
                pool_address,
                mint,
                quote_mint,
                trader,
                dex,
                is_buy,
                sol_amount,
                token_amount,
                token_decimals,
                signature,
                creator,
                token_program,
                ..
            } => MarketEventKind::Trade {
                pool_address: pool_address.to_string(),
                mint: mint.to_string(),
                quote_mint: quote_mint.to_string(),
                trader: trader.to_string(),
                is_buy: *is_buy,
                sol_amount: *sol_amount,
                token_amount: *token_amount,
                token_decimals: *token_decimals,
                signature: Some(signature.clone()),
                dex: dex.to_string(),
                creator: creator.map(|c| c.to_string()),
                token_program: token_program.map(|tp| tp.to_string()),
            },
            ParsedDexEvent::LiquidityRemoved {
                pool_address,
                mint,
                sol_amount,
                token_amount,
                signature,
                ..
            } => MarketEventKind::LiquidityRemoved {
                pool_address: pool_address.to_string(),
                mint: mint.to_string(),
                sol_amount: *sol_amount,
                token_amount: *token_amount,
                signature: Some(signature.clone()),
            },
            // BondingCurveUpdate is handled specially in market_data.rs
            // It doesn't map to a single MarketEventKind - instead we use it to
            // populate the creator_cache and emit DevWalletIdentified events
            ParsedDexEvent::BondingCurveUpdate { pool_address, .. } => {
                // Return a placeholder - this should not be called directly
                // market_data.rs handles BondingCurveUpdate separately
                MarketEventKind::PoolCreated {
                    pool_address: pool_address.to_string(),
                    base_mint: String::new(),
                    quote_mint: String::new(),
                    dex: "pumpfun".to_string(),
                    initial_liquidity_sol: None,
                }
            }
        }
    }

    /// Get slot from event
    pub fn slot(&self) -> u64 {
        match self {
            ParsedDexEvent::PoolCreated { slot, .. } => *slot,
            ParsedDexEvent::Trade { slot, .. } => *slot,
            ParsedDexEvent::LiquidityRemoved { slot, .. } => *slot,
            ParsedDexEvent::BondingCurveUpdate { slot, .. } => *slot,
        }
    }
}

// ============================================================================
// Meteora DLMM Parsing
// ============================================================================

fn parse_meteora_transaction(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    if update.instruction_data.len() < 8 {
        trace!(sig = %update.signature, "Meteora: instruction_data too short (< 8)");
        return None;
    }

    let disc = &update.instruction_data[0..8];
    if disc != METEORA_SWAP {
        trace!(sig = %update.signature, disc = ?disc, "Meteora: discriminator mismatch");
        return None;
    }

    // Meteora swap accounts (simplified, bin arrays ignored):
    // [0]: LB Pair (pool)
    // [1]: Reserve X
    // [2]: Reserve Y
    // [3]: User token X
    // [4]: User token Y
    // [5]: User (TRADER!)
    // [6]: Token program
    // ... bin arrays ...

    if update.instruction_accounts.len() < 7 {
        trace!(sig = %update.signature, account_count = update.instruction_accounts.len(), "Meteora: insufficient accounts (< 7)");
        return None;
    }

    let pool_address = update.instruction_accounts[0];
    let trader = update
        .account_keys
        .first()
        .copied()
        .unwrap_or(update.instruction_accounts[5]);

    // Parse amounts: discriminator(8) + amount_in(8) + min_out(8)
    if update.instruction_data.len() < 24 {
        return None;
    }

    let _amount_in = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let _min_out = u64::from_le_bytes(update.instruction_data[16..24].try_into().ok()?);

    // Determine direction from the two user token accounts, not from an arbitrary
    // increasing balance. On a SELL the non-WSOL user account decreases while WSOL
    // increases; the old increasing-non-WSOL heuristic therefore fell back to
    // Pubkey::default() and published the system program as the trade mint.
    let user_token_delta = |account: &Pubkey| -> Option<(Pubkey, i128)> {
        let account_index =
            u8::try_from(update.account_keys.iter().position(|key| key == account)?).ok()?;
        let post = update
            .post_token_balances
            .iter()
            .find(|balance| balance.account_index == account_index)?;
        let pre = update
            .pre_token_balances
            .iter()
            .find(|balance| balance.account_index == account_index && balance.mint == post.mint)?;
        let pre_amount = pre.ui_token_amount.amount.parse::<u64>().ok()?;
        let post_amount = post.ui_token_amount.amount.parse::<u64>().ok()?;
        let mint = Pubkey::from_str(&post.mint).ok()?;
        Some((mint, i128::from(post_amount) - i128::from(pre_amount)))
    };

    let (mint_x, delta_x) = user_token_delta(&update.instruction_accounts[3])?;
    let (mint_y, delta_y) = user_token_delta(&update.instruction_accounts[4])?;
    if mint_x == mint_y || delta_x == 0 || delta_y == 0 || delta_x.signum() == delta_y.signum() {
        debug!(
            sig = %update.signature,
            mint_x = %mint_x,
            mint_y = %mint_y,
            delta_x,
            delta_y,
            "Meteora: ambiguous user token balance deltas"
        );
        return None;
    }

    let sol_mint = *SOL_MINT_PUBKEY;
    let (base_mint, quote_mint, is_buy, sol_amount, token_amount) = if mint_x == sol_mint {
        if delta_x < 0 {
            (
                mint_y,
                mint_x,
                true,
                delta_x.unsigned_abs() as u64,
                delta_y.unsigned_abs() as u64,
            )
        } else {
            (
                mint_y,
                mint_x,
                false,
                delta_x.unsigned_abs() as u64,
                delta_y.unsigned_abs() as u64,
            )
        }
    } else if mint_y == sol_mint {
        if delta_y < 0 {
            (
                mint_x,
                mint_y,
                true,
                delta_y.unsigned_abs() as u64,
                delta_x.unsigned_abs() as u64,
            )
        } else {
            (
                mint_x,
                mint_y,
                false,
                delta_y.unsigned_abs() as u64,
                delta_x.unsigned_abs() as u64,
            )
        }
    } else if delta_x < 0 {
        // Preserve non-SOL pair provenance. Downstream strategies must reject
        // quote_mint != WSOL rather than treating the quote amount as lamports.
        (
            mint_y,
            mint_x,
            true,
            delta_x.unsigned_abs() as u64,
            delta_y.unsigned_abs() as u64,
        )
    } else {
        (
            mint_x,
            mint_y,
            true,
            delta_y.unsigned_abs() as u64,
            delta_x.unsigned_abs() as u64,
        )
    };

    debug!(
        pool = %pool_address,
        trader = %trader,
        is_buy = is_buy,
        sol_amount = sol_amount,
        token_amount = token_amount,
        sig = %update.signature,
        "Meteora DLMM swap detected"
    );

    let token_decimals = get_token_decimals(&update.post_token_balances, &base_mint);

    Some(ParsedDexEvent::Trade {
        pool_address,
        mint: base_mint,
        quote_mint,
        trader,
        dex: DexType::MeteoraDlmm,
        is_buy,
        sol_amount,
        token_amount,
        token_decimals,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: None,
        creator: None,
        token_program: None, // Meteora uses SPL Token, not relevant for momentum trading
        pump_amm_sell_requires_cashback_remaining: false,
        pump_amm_sell_cashback_third_meta: None,
        pump_amm_sell_extended_tail_0: None,
        pump_amm_sell_extended_tail_1: None,
        pump_amm_sell_extended_fee_tail_0: None,
        pump_amm_sell_extended_fee_tail_1: None,
        pump_amm_sell_requires_fee_tail: false,
        pump_amm_sell_requires_pre_fee_metas: false,
        pump_amm_sell_pre_fee_meta_1: None,
    })
}

// ============================================================================
// Raydium CPMM Parsing
// ============================================================================

fn parse_raydium_cpmm_transaction(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    if update.instruction_data.len() < 8 {
        trace!(sig = %update.signature, "Raydium CPMM: instruction_data too short (< 8)");
        return None;
    }

    let disc = &update.instruction_data[0..8];
    if disc != RAYDIUM_CPMM_SWAP {
        trace!(sig = %update.signature, disc = ?disc, "Raydium CPMM: discriminator mismatch");
        return None;
    }

    // Raydium CPMM swap accounts:
    // [0]: Payer
    // [1]: Authority
    // [2]: Pool config
    // [3]: Pool state
    // [4]: User source token
    // [5]: User destination token
    // [6]: Pool vault 0
    // [7]: Pool vault 1
    // [8]: Token program 0
    // [9]: Token program 1
    // [10]: Vault 0 mint
    // [11]: Vault 1 mint

    if update.instruction_accounts.len() < 12 {
        trace!(sig = %update.signature, account_count = update.instruction_accounts.len(), "Raydium CPMM: insufficient accounts (< 12)");
        return None;
    }

    let pool_address = update.instruction_accounts[3];
    let trader = update
        .account_keys
        .first()
        .copied()
        .unwrap_or(update.instruction_accounts[0]);

    // Parse amounts: discriminator(8) + amount_in(8) + min_out(8)
    if update.instruction_data.len() < 24 {
        return None;
    }

    let amount_in = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let _min_out = u64::from_le_bytes(update.instruction_data[16..24].try_into().ok()?);

    // Extract mints from accounts
    let vault_0_mint = update.instruction_accounts.get(10).copied()?;
    let vault_1_mint = update.instruction_accounts.get(11).copied()?;

    let sol_mint = *SOL_MINT_PUBKEY;

    // Determine which is base (non-SOL) and which is quote (SOL)
    let (base_mint, is_buy) = if vault_0_mint == sol_mint {
        (vault_1_mint, true) // Vault 0 = SOL, so buying vault 1 token
    } else if vault_1_mint == sol_mint {
        (vault_0_mint, false) // Vault 1 = SOL, so selling vault 0 token
    } else {
        // Non-SOL pair, use first vault as base
        (vault_0_mint, true)
    };

    // Derive actual quote_mint from vault mints (instead of hardcoding SOL).
    // For non-SOL pairs (e.g., TOKEN/USDC) this ensures arb-strategy correctly
    // filters them out instead of comparing incompatible quote currencies.
    let quote_mint = if base_mint == vault_0_mint {
        vault_1_mint
    } else {
        vault_0_mint
    };

    // Calculate actual amounts from balance changes
    let (sol_amount, token_amount) = if is_buy {
        let tokens_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &base_mint,
        )
        .unwrap_or(0);
        (amount_in, tokens_received)
    } else {
        // SELL: SOL received is in native balances, not token_balances!
        let sol_received = calculate_native_balance_change(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &trader,
        )
        .unwrap_or(0);
        (sol_received, amount_in)
    };

    debug!(
        pool = %pool_address,
        trader = %trader,
        is_buy = is_buy,
        sol_amount = sol_amount,
        token_amount = token_amount,
        sig = %update.signature,
        "Raydium CPMM swap detected"
    );

    let token_decimals = get_token_decimals(&update.post_token_balances, &base_mint);

    Some(ParsedDexEvent::Trade {
        pool_address,
        mint: base_mint,
        quote_mint,
        trader,
        dex: DexType::RaydiumCpmm,
        is_buy,
        sol_amount,
        token_amount,
        token_decimals,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: None,
        creator: None,
        token_program: None, // Raydium CPMM uses SPL Token, not relevant for momentum trading
        pump_amm_sell_requires_cashback_remaining: false,
        pump_amm_sell_cashback_third_meta: None,
        pump_amm_sell_extended_tail_0: None,
        pump_amm_sell_extended_tail_1: None,
        pump_amm_sell_extended_fee_tail_0: None,
        pump_amm_sell_extended_fee_tail_1: None,
        pump_amm_sell_requires_fee_tail: false,
        pump_amm_sell_requires_pre_fee_metas: false,
        pump_amm_sell_pre_fee_meta_1: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solana::dex::pumpfun::PUMPFUN_BUY_EXACT_SOL_IN_DISCRIMINATOR;
    use crate::solana::geyser_listener::{
        GeyserTransactionUpdate, InnerInstruction, TokenAmount, TokenBalance,
    };
    use std::time::Instant;

    fn meteora_user_delta_update(is_buy: bool) -> (GeyserTransactionUpdate, Pubkey) {
        let pool = Pubkey::new_unique();
        let reserve_x = Pubkey::new_unique();
        let reserve_y = Pubkey::new_unique();
        let user_wsol = Pubkey::new_unique();
        let user_token = Pubkey::new_unique();
        let trader = Pubkey::new_unique();
        let token_program = Pubkey::new_unique();
        let token_mint = Pubkey::new_unique();
        let wsol = *SOL_MINT_PUBKEY;
        let account_keys = vec![
            pool,
            reserve_x,
            reserve_y,
            user_wsol,
            user_token,
            trader,
            token_program,
        ];
        let instruction_accounts = account_keys.clone();
        let (wsol_pre, wsol_post, token_pre, token_post) = if is_buy {
            (1_000_000_000, 900_000_000, 0, 500_000_000)
        } else {
            (900_000_000, 1_000_000_000, 500_000_000, 0)
        };
        let balances = |account_index: u8, mint: Pubkey, decimals: u8, amount: u64| TokenBalance {
            account_index,
            mint: mint.to_string(),
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals,
                amount: amount.to_string(),
            },
            program_id: None,
            owner: None,
        };
        let mut instruction_data = METEORA_SWAP.to_vec();
        instruction_data.extend_from_slice(&100_000_000u64.to_le_bytes());
        instruction_data.extend_from_slice(&0u64.to_le_bytes());
        (
            GeyserTransactionUpdate {
                signature: format!("meteora-{}", if is_buy { "buy" } else { "sell" }),
                slot: 42,
                account_keys,
                instruction_accounts,
                instruction_data,
                inner_instructions: vec![],
                pre_token_balances: vec![
                    balances(3, wsol, 9, wsol_pre),
                    balances(4, token_mint, 6, token_pre),
                ],
                post_token_balances: vec![
                    balances(3, wsol, 9, wsol_post),
                    balances(4, token_mint, 6, token_post),
                ],
                pre_balances: vec![0; 7],
                post_balances: vec![0; 7],
                fee_lamports: 0,
                compute_units_consumed: None,
                grpc_recv_at: Instant::now(),
            },
            token_mint,
        )
    }

    #[test]
    fn meteora_buy_uses_increasing_non_wsol_user_mint() {
        let (update, token_mint) = meteora_user_delta_update(true);
        let ParsedDexEvent::Trade {
            mint,
            quote_mint,
            is_buy,
            sol_amount,
            token_amount,
            ..
        } = parse_meteora_transaction(&update).expect("meteora buy")
        else {
            panic!("expected trade")
        };
        assert_eq!(mint, token_mint);
        assert_eq!(quote_mint, *SOL_MINT_PUBKEY);
        assert!(is_buy);
        assert_eq!(sol_amount, 100_000_000);
        assert_eq!(token_amount, 500_000_000);
    }

    #[test]
    fn meteora_sell_uses_decreasing_non_wsol_user_mint() {
        let (update, token_mint) = meteora_user_delta_update(false);
        let ParsedDexEvent::Trade {
            mint,
            quote_mint,
            is_buy,
            sol_amount,
            token_amount,
            ..
        } = parse_meteora_transaction(&update).expect("meteora sell")
        else {
            panic!("expected trade")
        };
        assert_eq!(mint, token_mint);
        assert_ne!(mint, Pubkey::default());
        assert_eq!(quote_mint, *SOL_MINT_PUBKEY);
        assert!(!is_buy);
        assert_eq!(sol_amount, 100_000_000);
        assert_eq!(token_amount, 500_000_000);
    }

    #[test]
    fn selected_instruction_program_wins_in_multi_dex_transaction() {
        let orca = Pubkey::from_str(ORCA_WHIRLPOOL).unwrap();
        let meteora = Pubkey::from_str(METEORA_DLMM).unwrap();
        let pool = Pubkey::new_unique();
        let token_mint = Pubkey::new_unique();
        let sol_mint = *SOL_MINT_PUBKEY;
        let token_program =
            Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let authority = Pubkey::new_unique();
        let trader = Pubkey::new_unique();
        let instruction_accounts = vec![
            token_program,
            authority,
            pool,
            trader,
            crate::solana::geyser_listener::INSTRUCTION_PROGRAM_TRAILER,
            orca,
        ];
        let account_keys = vec![meteora, orca, token_program, authority, pool, trader];
        let mut instruction_data = ORCA_SWAP.to_vec();
        instruction_data.extend_from_slice(&[0u8; 34]);
        let update = GeyserTransactionUpdate {
            signature: "multi-dex-router".to_string(),
            slot: 1,
            account_keys,
            instruction_accounts,
            instruction_data: instruction_data.clone(),
            inner_instructions: vec![InnerInstruction {
                program_id_index: 1,
                accounts: vec![2, 3, 4, 5],
                data: instruction_data,
            }],
            pre_token_balances: vec![],
            post_token_balances: vec![],
            pre_balances: vec![],
            post_balances: vec![],
            fee_lamports: 0,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };
        let lookup = |candidate: &Pubkey| {
            (*candidate == pool).then_some(OrcaPoolInfo {
                token_mint_a: sol_mint,
                token_mint_b: token_mint,
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
                tick_current_index: Some(0),
                tick_spacing: Some(64),
                token_a_program: Some(token_program),
                token_b_program: Some(token_program),
            })
        };

        let parsed = parse_transaction_update_with_pool_lookup(&update, Some(&lookup));
        assert!(matches!(
            parsed,
            Some(ParsedDexEvent::Trade {
                pool_address,
                dex: DexType::OrcaWhirlpool,
                ..
            }) if pool_address == pool
        ));
    }

    #[test]
    fn unique_valid_layout_wins_when_legacy_update_lacks_program_provenance() {
        let orca = Pubkey::from_str(ORCA_WHIRLPOOL).unwrap();
        let meteora = Pubkey::from_str(METEORA_DLMM).unwrap();
        let pool = Pubkey::new_unique();
        let token_mint = Pubkey::new_unique();
        let token_program =
            Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let mut instruction_data = ORCA_SWAP.to_vec();
        instruction_data.extend_from_slice(&[0u8; 34]);
        let update = GeyserTransactionUpdate {
            signature: "legacy-multi-dex".to_string(),
            slot: 1,
            account_keys: vec![meteora, orca, token_program, Pubkey::new_unique(), pool],
            instruction_accounts: vec![
                token_program,
                Pubkey::new_unique(),
                pool,
                Pubkey::new_unique(),
            ],
            instruction_data,
            inner_instructions: vec![],
            pre_token_balances: vec![],
            post_token_balances: vec![],
            pre_balances: vec![],
            post_balances: vec![],
            fee_lamports: 0,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };
        let lookup = |candidate: &Pubkey| {
            (*candidate == pool).then_some(OrcaPoolInfo {
                token_mint_a: *SOL_MINT_PUBKEY,
                token_mint_b: token_mint,
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
                tick_current_index: Some(0),
                tick_spacing: Some(64),
                token_a_program: Some(token_program),
                token_b_program: Some(token_program),
            })
        };

        assert!(matches!(
            parse_transaction_update_with_pool_lookup(&update, Some(&lookup)),
            Some(ParsedDexEvent::Trade {
                dex: DexType::OrcaWhirlpool,
                ..
            })
        ));
    }

    #[test]
    fn test_dex_type_display() {
        assert_eq!(DexType::RaydiumAmmV4.to_string(), "raydium");
        assert_eq!(DexType::OrcaWhirlpool.to_string(), "orca");
        assert_eq!(DexType::PumpFun.to_string(), "pumpfun");
        assert_eq!(DexType::PumpFunAmm.to_string(), "pump_amm");
    }

    #[test]
    fn test_pumpfun_amm_create_pool_parsing() {
        // Create minimal create_pool TX: discriminator + 18 accounts
        let pool = Pubkey::new_unique();
        let global_config = Pubkey::new_unique();
        let creator = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let mut instruction_accounts = vec![pool, global_config, creator, base_mint, quote_mint];
        for _ in 0..13 {
            instruction_accounts.push(Pubkey::new_unique());
        }
        let mut instruction_data = PUMPFUN_AMM_CREATE_POOL.to_vec();
        instruction_data.extend_from_slice(&[0u8; 24]); // index(2) + base_in(8) + quote_in(8)

        let pumpfun_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).unwrap();
        let mut account_keys = instruction_accounts.clone();
        account_keys.push(pumpfun_amm_program); // Required for parse to dispatch to PumpFunAmm

        let update = GeyserTransactionUpdate {
            signature: "test_sig".to_string(),
            slot: 100,
            account_keys,
            instruction_accounts,
            instruction_data,
            inner_instructions: vec![],
            pre_token_balances: vec![],
            post_token_balances: vec![],
            pre_balances: vec![],
            post_balances: vec![],
            fee_lamports: 5000,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };

        let parsed = parse_transaction_update(&update);
        assert!(parsed.is_some());
        let event = parsed.unwrap();
        match &event {
            ParsedDexEvent::PoolCreated {
                pool_address: p,
                base_mint: b,
                quote_mint: q,
                dex,
                pool_accounts,
                creator: c,
                ..
            } => {
                assert_eq!(p, &pool);
                assert_eq!(b, &base_mint);
                assert_eq!(q, &quote_mint);
                assert_eq!(dex, &DexType::PumpFunAmm);
                assert_eq!(c, &Some(creator));
                assert!(
                    pool_accounts.is_none(),
                    "create_pool must not carry synthetic pool_accounts (wait for swap-verified path)"
                );
            }
            _ => panic!("expected PoolCreated, got {:?}", event),
        }
    }

    #[test]
    fn test_pumpfun_amm_sell_trade_includes_verified_pool_accounts() {
        let pool_market = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let global_config = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let user_base = Pubkey::new_unique();
        let user_quote = Pubkey::new_unique();
        let pool_base_vault = Pubkey::new_unique();
        let pool_quote_vault = Pubkey::new_unique();
        let protocol_fee_recipient = Pubkey::new_unique();
        let protocol_fee_recipient_ta = Pubkey::new_unique();
        let spl_token = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let system = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let ata_program = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();
        let event_authority = Pubkey::new_unique();
        let pumpfun_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).unwrap();
        let coin_creator_vault_ata = Pubkey::new_unique();
        let coin_creator_vault_authority = Pubkey::new_unique();
        let fee_config = Pubkey::from_str("5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx").unwrap();
        let fee_program = Pubkey::from_str("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ").unwrap();

        let instruction_accounts = vec![
            pool_market,
            user,
            global_config,
            base_mint,
            quote_mint,
            user_base,
            user_quote,
            pool_base_vault,
            pool_quote_vault,
            protocol_fee_recipient,
            protocol_fee_recipient_ta,
            spl_token,
            spl_token,
            system,
            ata_program,
            event_authority,
            pumpfun_amm_program,
            coin_creator_vault_ata,
            coin_creator_vault_authority,
            fee_config,
            fee_program,
        ];
        assert_eq!(instruction_accounts.len(), 21);

        let sell_disc = anchor_disc("sell");
        let mut instruction_data = sell_disc.to_vec();
        let amount_in: u64 = 1_000_000;
        instruction_data.extend_from_slice(&amount_in.to_le_bytes());
        instruction_data.extend_from_slice(&0u64.to_le_bytes()); // min_out

        let mut account_keys = instruction_accounts.clone();
        account_keys.insert(0, user);

        let base_mint_str = base_mint.to_string();
        let pre_token = vec![TokenBalance {
            account_index: 5,
            mint: base_mint_str.clone(),
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "2000000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];
        let post_token = vec![TokenBalance {
            account_index: 5,
            mint: base_mint_str,
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "1000000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];

        let pre_balances = vec![1_000_000_000u64; account_keys.len()];
        let mut post_balances = pre_balances.clone();
        post_balances[0] = 1_000_050_000_000;

        let update = GeyserTransactionUpdate {
            signature: "sell_synth_sig".to_string(),
            slot: 200,
            account_keys,
            instruction_accounts,
            instruction_data,
            inner_instructions: vec![],
            pre_token_balances: pre_token,
            post_token_balances: post_token,
            pre_balances,
            post_balances,
            fee_lamports: 5000,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };

        let parsed = parse_transaction_update(&update);
        let event = parsed.expect("sell parse");
        match &event {
            ParsedDexEvent::Trade {
                dex,
                pool_accounts,
                pump_amm_sell_requires_cashback_remaining,
                ..
            } => {
                assert_eq!(dex, &DexType::PumpFunAmm);
                assert!(!pump_amm_sell_requires_cashback_remaining);
                let accts = pool_accounts
                    .as_ref()
                    .expect("swap path must carry pool_accounts");
                assert_eq!(accts.len(), 14);
                assert_eq!(accts[0], pool_market);
                assert_eq!(accts[6], protocol_fee_recipient);
            }
            _ => panic!("expected Trade, got {:?}", event),
        }
    }

    #[test]
    fn test_pumpfun_amm_sell_24_accounts_sets_cashback_remaining_flag() {
        let pool_market = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let global_config = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let user_base = Pubkey::new_unique();
        let user_quote = Pubkey::new_unique();
        let pool_base_vault = Pubkey::new_unique();
        let pool_quote_vault = Pubkey::new_unique();
        let protocol_fee_recipient = Pubkey::new_unique();
        let protocol_fee_recipient_ta = Pubkey::new_unique();
        let spl_token = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let system = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let ata_program = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();
        let event_authority = Pubkey::new_unique();
        let pumpfun_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).unwrap();
        let coin_creator_vault_ata = Pubkey::new_unique();
        let coin_creator_vault_authority = Pubkey::new_unique();
        let fee_config = Pubkey::from_str("5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx").unwrap();
        let fee_program = Pubkey::from_str("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ").unwrap();

        let tail0 = Pubkey::new_unique();
        let tail1 = Pubkey::new_unique();
        let third_trailing = Pubkey::new_unique();
        let instruction_accounts = vec![
            pool_market,
            user,
            global_config,
            base_mint,
            quote_mint,
            user_base,
            user_quote,
            pool_base_vault,
            pool_quote_vault,
            protocol_fee_recipient,
            protocol_fee_recipient_ta,
            spl_token,
            spl_token,
            system,
            ata_program,
            event_authority,
            pumpfun_amm_program,
            coin_creator_vault_ata,
            coin_creator_vault_authority,
            fee_config,
            fee_program,
            tail0,
            tail1,
            third_trailing,
        ];
        assert_eq!(instruction_accounts.len(), 24);

        let sell_disc = anchor_disc("sell");
        let mut instruction_data = sell_disc.to_vec();
        instruction_data.extend_from_slice(&1_000_000u64.to_le_bytes());
        instruction_data.extend_from_slice(&0u64.to_le_bytes());

        let mut account_keys = instruction_accounts.clone();
        account_keys.insert(0, user);

        let base_mint_str = base_mint.to_string();
        let pre_token = vec![TokenBalance {
            account_index: 5,
            mint: base_mint_str.clone(),
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "2000000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];
        let post_token = vec![TokenBalance {
            account_index: 5,
            mint: base_mint_str,
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "1000000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];

        let pre_balances = vec![1_000_000_000u64; account_keys.len()];
        let mut post_balances = pre_balances.clone();
        post_balances[0] = 1_000_050_000_000;

        let update = GeyserTransactionUpdate {
            signature: "sell_ext_synth_sig".to_string(),
            slot: 201,
            account_keys,
            instruction_accounts,
            instruction_data,
            inner_instructions: vec![],
            pre_token_balances: pre_token,
            post_token_balances: post_token,
            pre_balances,
            post_balances,
            fee_lamports: 5000,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };

        let event = parse_transaction_update(&update).expect("extended sell parse");
        match event {
            ParsedDexEvent::Trade {
                pump_amm_sell_requires_cashback_remaining,
                pump_amm_sell_cashback_third_meta,
                pump_amm_sell_extended_tail_0,
                pump_amm_sell_extended_tail_1,
                ..
            } => {
                assert!(pump_amm_sell_requires_cashback_remaining);
                assert_eq!(pump_amm_sell_cashback_third_meta, Some(third_trailing));
                assert_eq!(pump_amm_sell_extended_tail_0, Some(tail0));
                assert_eq!(pump_amm_sell_extended_tail_1, Some(tail1));
            }
            _ => panic!("expected Trade"),
        }
    }

    #[test]
    fn test_pumpfun_amm_sell_26_accounts_sets_cashback_remaining_flag() {
        let pool_market = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let global_config = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let user_base = Pubkey::new_unique();
        let user_quote = Pubkey::new_unique();
        let pool_base_vault = Pubkey::new_unique();
        let pool_quote_vault = Pubkey::new_unique();
        let protocol_fee_recipient = Pubkey::new_unique();
        let protocol_fee_recipient_ta = Pubkey::new_unique();
        let spl_token = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let system = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let ata_program = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();
        let event_authority = Pubkey::new_unique();
        let pumpfun_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).unwrap();
        let coin_creator_vault_ata = Pubkey::new_unique();
        let coin_creator_vault_authority = Pubkey::new_unique();
        let fee_config = Pubkey::from_str("5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx").unwrap();
        let fee_program = Pubkey::from_str("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ").unwrap();

        let tail0 = Pubkey::new_unique();
        let tail1 = Pubkey::new_unique();
        let third_trailing = Pubkey::new_unique();
        let trailing_fee_recipient = Pubkey::new_unique();
        let trailing_fee_recipient_ta = Pubkey::new_unique();
        let instruction_accounts = vec![
            pool_market,
            user,
            global_config,
            base_mint,
            quote_mint,
            user_base,
            user_quote,
            pool_base_vault,
            pool_quote_vault,
            protocol_fee_recipient,
            protocol_fee_recipient_ta,
            spl_token,
            spl_token,
            system,
            ata_program,
            event_authority,
            pumpfun_amm_program,
            coin_creator_vault_ata,
            coin_creator_vault_authority,
            fee_config,
            fee_program,
            tail0,
            tail1,
            third_trailing,
            trailing_fee_recipient,
            trailing_fee_recipient_ta,
        ];
        assert_eq!(instruction_accounts.len(), 26);

        let sell_disc = anchor_disc("sell");
        let mut instruction_data = sell_disc.to_vec();
        instruction_data.extend_from_slice(&1_000_000u64.to_le_bytes());
        instruction_data.extend_from_slice(&0u64.to_le_bytes());

        let mut account_keys = instruction_accounts.clone();
        account_keys.insert(0, user);

        let base_mint_str = base_mint.to_string();
        let pre_token = vec![TokenBalance {
            account_index: 5,
            mint: base_mint_str.clone(),
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "2000000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];
        let post_token = vec![TokenBalance {
            account_index: 5,
            mint: base_mint_str,
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "1000000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];

        let pre_balances = vec![1_000_000_000u64; account_keys.len()];
        let mut post_balances = pre_balances.clone();
        post_balances[0] = 1_000_050_000_000;

        let update = GeyserTransactionUpdate {
            signature: "sell_26_synth_sig".to_string(),
            slot: 202,
            account_keys,
            instruction_accounts,
            instruction_data,
            inner_instructions: vec![],
            pre_token_balances: pre_token,
            post_token_balances: post_token,
            pre_balances,
            post_balances,
            fee_lamports: 5000,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };

        let event = parse_transaction_update(&update).expect("26-account sell parse");
        match event {
            ParsedDexEvent::Trade {
                pump_amm_sell_requires_cashback_remaining,
                pump_amm_sell_cashback_third_meta,
                pump_amm_sell_extended_tail_0,
                pump_amm_sell_extended_tail_1,
                pool_accounts,
                ..
            } => {
                assert!(pump_amm_sell_requires_cashback_remaining);
                assert_eq!(pump_amm_sell_cashback_third_meta, Some(third_trailing));
                assert_eq!(pump_amm_sell_extended_tail_0, Some(tail0));
                assert_eq!(pump_amm_sell_extended_tail_1, Some(tail1));
                let accts = pool_accounts.expect("pool_accounts");
                assert_eq!(accts[6], protocol_fee_recipient);
            }
            _ => panic!("expected Trade"),
        }
    }

    #[test]
    fn test_pumpfun_amm_inner_cpi_sell_26_accounts() {
        use crate::solana::geyser_listener::InnerInstruction;

        let pool_market = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let global_config = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let spl_token = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let system = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let ata_program = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();
        let pumpfun_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).unwrap();
        let fee_config = Pubkey::from_str("5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx").unwrap();
        let fee_program = Pubkey::from_str("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ").unwrap();

        let sell_accounts = vec![
            pool_market,
            user,
            global_config,
            base_mint,
            quote_mint,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            spl_token,
            spl_token,
            system,
            ata_program,
            Pubkey::new_unique(),
            pumpfun_amm_program,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            fee_config,
            fee_program,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(sell_accounts.len(), 26);

        let jupiter = Pubkey::new_unique();
        let mut account_keys = vec![user, jupiter];
        account_keys.extend(sell_accounts.iter().copied());

        let pump_amm_idx = account_keys
            .iter()
            .position(|k| *k == pumpfun_amm_program)
            .unwrap();
        let sell_account_indices: Vec<u8> = sell_accounts
            .iter()
            .map(|pk| account_keys.iter().position(|k| k == pk).unwrap() as u8)
            .collect();

        let sell_disc = anchor_disc("sell");
        let mut sell_data = sell_disc.to_vec();
        sell_data.extend_from_slice(&500_000u64.to_le_bytes());
        sell_data.extend_from_slice(&0u64.to_le_bytes());

        let base_mint_str = base_mint.to_string();
        let pre_token = vec![TokenBalance {
            account_index: sell_account_indices[5],
            mint: base_mint_str.clone(),
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "2000000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];
        let post_token = vec![TokenBalance {
            account_index: sell_account_indices[5],
            mint: base_mint_str,
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "1000000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];

        let update = GeyserTransactionUpdate {
            signature: "inner_cpi_sell_sig".to_string(),
            slot: 203,
            account_keys: account_keys.clone(),
            instruction_accounts: vec![user],
            instruction_data: vec![0u8; 8],
            inner_instructions: vec![InnerInstruction {
                program_id_index: pump_amm_idx as u8,
                accounts: sell_account_indices,
                data: sell_data,
            }],
            pre_token_balances: pre_token,
            post_token_balances: post_token,
            pre_balances: vec![1_000_000_000u64; account_keys.len()],
            post_balances: vec![1_000_050_000_000u64; account_keys.len()],
            fee_lamports: 5000,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };

        let event = parse_transaction_update(&update).expect("inner CPI sell");
        match event {
            ParsedDexEvent::Trade {
                is_buy,
                pump_amm_sell_requires_cashback_remaining,
                ..
            } => {
                assert!(!is_buy);
                assert!(pump_amm_sell_requires_cashback_remaining);
            }
            _ => panic!("expected Trade"),
        }
    }

    #[test]
    fn mogemoji_eg_hj_buy_slot_594_tokens_per_sol_chart_compatible() {
        const SPENDABLE_SOL_IN: u64 = 1_062_000;
        const TOKEN_RAW: u64 = 4_096_007_510u64;

        let trader = Pubkey::new_unique();
        let g0 = Pubkey::new_unique();
        let fee_r = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let bonding_curve = Pubkey::new_unique();
        let assoc_bc = Pubkey::new_unique();
        let user_ata = Pubkey::new_unique();
        let pumpfun = Pubkey::from_str(PUMPFUN_PROGRAM).unwrap();

        let instruction_accounts = vec![g0, fee_r, mint, bonding_curve, assoc_bc, user_ata, trader];

        let mut account_keys = vec![trader];
        account_keys.extend_from_slice(&instruction_accounts);
        account_keys.push(pumpfun);

        let mut instruction_data = PUMPFUN_BUY_EXACT_SOL_IN_DISCRIMINATOR.to_vec();
        instruction_data.extend_from_slice(&SPENDABLE_SOL_IN.to_le_bytes());
        instruction_data.extend_from_slice(&1u64.to_le_bytes());
        instruction_data.push(0u8);

        let mint_str = mint.to_string();
        let spl_token = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let user_ata_idx = account_keys.iter().position(|k| *k == user_ata).unwrap() as u8;

        let pre_token = vec![TokenBalance {
            account_index: user_ata_idx,
            mint: mint_str.clone(),
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "0".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];
        let post_token = vec![TokenBalance {
            account_index: user_ata_idx,
            mint: mint_str,
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: TOKEN_RAW.to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];

        let pre_balances = vec![2_005_000u64; account_keys.len()];
        let mut post_balances = pre_balances.clone();
        post_balances[0] = 0;

        let update = GeyserTransactionUpdate {
            signature: "3PxnUrBbPWHqGKeQ1Gii8kYcGsVgE64v8YRjJR7utGqEMr1u2k9gGG3w6Mwb6LHJNpxVMbwae2dnrfwdc1YbJfcK"
                .to_string(),
            slot: 421_716_594,
            account_keys,
            instruction_accounts,
            instruction_data,
            inner_instructions: vec![],
            pre_token_balances: pre_token,
            post_token_balances: post_token,
            pre_balances,
            post_balances,
            fee_lamports: 5000,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };

        let event = parse_transaction_update(&update).expect("parse buy_exact_sol_in");
        match event {
            ParsedDexEvent::Trade {
                sol_amount,
                token_amount,
                token_decimals,
                is_buy,
                dex,
                ..
            } => {
                assert_eq!(dex, DexType::PumpFun);
                assert!(is_buy);
                assert_eq!(sol_amount, SPENDABLE_SOL_IN);
                assert_eq!(token_amount, TOKEN_RAW);
                assert_eq!(token_decimals, 6);
                let token_ui = token_amount as f64 / 10f64.powi(i32::from(token_decimals));
                let sol_ui = sol_amount as f64 / 1e9;
                let tps = token_ui / sol_ui;
                let expected_mid = 3_856_425f64;
                let lo = expected_mid * 0.98;
                let hi = expected_mid * 1.02;
                assert!(
                    tps >= lo && tps <= hi,
                    "tps {tps} not near chart-compatible ~3.86M (lo={lo}, hi={hi})"
                );
            }
            _ => panic!("expected Trade"),
        }
    }

    #[test]
    fn buy_exact_sol_in_discriminator_routed() {
        let trader = Pubkey::new_unique();
        let g0 = Pubkey::new_unique();
        let fee_r = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let bonding_curve = Pubkey::new_unique();
        let assoc_bc = Pubkey::new_unique();
        let user_ata = Pubkey::new_unique();
        let pumpfun = Pubkey::from_str(PUMPFUN_PROGRAM).unwrap();

        let instruction_accounts = vec![g0, fee_r, mint, bonding_curve, assoc_bc, user_ata, trader];
        let mut account_keys = vec![trader];
        account_keys.extend_from_slice(&instruction_accounts);
        account_keys.push(pumpfun);

        let spendable: u64 = 500_000;
        let mut instruction_data = PUMPFUN_BUY_EXACT_SOL_IN_DISCRIMINATOR.to_vec();
        instruction_data.extend_from_slice(&spendable.to_le_bytes());
        instruction_data.extend_from_slice(&1u64.to_le_bytes());
        instruction_data.push(0u8);

        let spl_token = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let mint_str = mint.to_string();
        let user_ata_idx = account_keys.iter().position(|k| *k == user_ata).unwrap() as u8;
        let pre_token = vec![TokenBalance {
            account_index: user_ata_idx,
            mint: mint_str.clone(),
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "0".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];
        let post_token = vec![TokenBalance {
            account_index: user_ata_idx,
            mint: mint_str,
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "1000000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];

        let update = GeyserTransactionUpdate {
            signature: "synth_buy_exact_sol".to_string(),
            slot: 1,
            account_keys,
            instruction_accounts,
            instruction_data,
            inner_instructions: vec![],
            pre_token_balances: pre_token,
            post_token_balances: post_token,
            pre_balances: vec![10_000_000; 9],
            post_balances: vec![10_000_000; 9],
            fee_lamports: 5000,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };

        let e = parse_transaction_update(&update).expect("routed");
        match e {
            ParsedDexEvent::Trade {
                sol_amount, is_buy, ..
            } => {
                assert!(is_buy);
                assert_eq!(sol_amount, spendable);
            }
            _ => panic!("expected Trade"),
        }
    }

    #[test]
    fn legacy_buy_curve_native_inflow_not_wallet_gross() {
        const PUMPFUN_BUY_DISC: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
        let trader = Pubkey::new_unique();
        let g0 = Pubkey::new_unique();
        let fee_r = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let bonding_curve = Pubkey::new_unique();
        let assoc_bc = Pubkey::new_unique();
        let user_ata = Pubkey::new_unique();
        let pumpfun = Pubkey::from_str(PUMPFUN_PROGRAM).unwrap();

        let instruction_accounts = vec![g0, fee_r, mint, bonding_curve, assoc_bc, user_ata, trader];
        let mut account_keys = vec![trader];
        account_keys.extend_from_slice(&instruction_accounts);
        account_keys.push(pumpfun);

        let curve_idx = account_keys
            .iter()
            .position(|k| *k == bonding_curve)
            .unwrap();

        let pre_balances = vec![10_000_000_000u64; account_keys.len()];
        let mut post_balances = pre_balances.clone();
        post_balances[0] = pre_balances[0] - 6_000_000;
        post_balances[curve_idx] = pre_balances[curve_idx] + 1_000_000;

        let mut instruction_data = PUMPFUN_BUY_DISC.to_vec();
        instruction_data.extend_from_slice(&500_000u64.to_le_bytes());
        instruction_data.extend_from_slice(&10_000_000u64.to_le_bytes());

        let spl_token = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let mint_str = mint.to_string();
        let user_ata_idx = account_keys.iter().position(|k| *k == user_ata).unwrap() as u8;
        let pre_token = vec![TokenBalance {
            account_index: user_ata_idx,
            mint: mint_str.clone(),
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "0".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];
        let post_token = vec![TokenBalance {
            account_index: user_ata_idx,
            mint: mint_str,
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "1000000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];

        let update = GeyserTransactionUpdate {
            signature: "legacy_buy_curve".to_string(),
            slot: 2,
            account_keys,
            instruction_accounts,
            instruction_data,
            inner_instructions: vec![],
            pre_token_balances: pre_token,
            post_token_balances: post_token,
            pre_balances,
            post_balances,
            fee_lamports: 5000,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };

        let e = parse_transaction_update(&update).expect("legacy buy");
        match e {
            ParsedDexEvent::Trade { sol_amount, .. } => {
                assert_eq!(
                    sol_amount, 1_000_000,
                    "must use bonding curve inflow, not wallet gross 6M"
                );
            }
            _ => panic!("expected Trade"),
        }
    }

    #[test]
    fn regression_raydium_buy_unchanged_uses_amount_in() {
        const LEGACY_BUY_DISC: u8 = 11;
        let trader = Pubkey::new_unique();
        let spl_token = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let raydium = Pubkey::from_str(RAYDIUM_AMM_V4).unwrap();
        let base_mint = Pubkey::new_unique();

        let mut instruction_accounts = vec![spl_token, Pubkey::new_unique()];
        while instruction_accounts.len() < 16 {
            instruction_accounts.push(Pubkey::new_unique());
        }
        instruction_accounts.push(trader);

        let mut account_keys = instruction_accounts.clone();
        account_keys.push(raydium);

        let amount_in: u64 = 12_345_678;
        let mut instruction_data = vec![LEGACY_BUY_DISC];
        instruction_data.extend_from_slice(&amount_in.to_le_bytes());
        instruction_data.extend_from_slice(&1u64.to_le_bytes());

        let mint_str = base_mint.to_string();
        let pre_token = vec![TokenBalance {
            account_index: 15,
            mint: mint_str.clone(),
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "0".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];
        let post_token = vec![TokenBalance {
            account_index: 15,
            mint: mint_str,
            ui_token_amount: TokenAmount {
                ui_amount: None,
                decimals: 6,
                amount: "999000".to_string(),
            },
            program_id: Some(spl_token.to_string()),
            owner: None,
        }];

        let update = GeyserTransactionUpdate {
            signature: "raydium_buy_regression".to_string(),
            slot: 3,
            account_keys,
            instruction_accounts,
            instruction_data,
            inner_instructions: vec![],
            pre_token_balances: pre_token,
            post_token_balances: post_token,
            pre_balances: vec![5_000_000_000; 18],
            post_balances: vec![5_000_000_000; 18],
            fee_lamports: 5000,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };

        let e = parse_transaction_update(&update).expect("raydium parse");
        match e {
            ParsedDexEvent::Trade {
                sol_amount,
                is_buy,
                dex,
                ..
            } => {
                assert_eq!(dex, DexType::RaydiumAmmV4);
                assert!(is_buy);
                assert_eq!(sol_amount, amount_in);
            }
            _ => panic!("expected Trade"),
        }
    }
}
