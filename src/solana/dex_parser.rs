//! DEX Transaction/Account Parsing for MarketEvents
//!
//! Public parsing functions for market-data service to convert raw Geyser
//! updates into structured MarketEvents (PoolCreated, Trade, etc.)
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.1
//! Supports: Raydium AMM V4, Orca Whirlpool, PumpFun

use crate::solana::dex::pumpfun::BondingCurveState;
use crate::solana::geyser_listener::{GeyserAccountUpdate, GeyserTransactionUpdate};
use rust_decimal::Decimal;
use solana_sdk::hash::hash;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::pubkey::Pubkey as SplPubkey;
use std::str::FromStr;
use tracing::{debug, info, trace};

fn to_spl_pubkey(p: &Pubkey) -> SplPubkey {
    SplPubkey::new_from_array(p.to_bytes())
}

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

    if update.account_keys.contains(&meteora) {
        return parse_meteora_transaction(update);
    }
    if update.account_keys.contains(&raydium_cpmm) {
        return parse_raydium_cpmm_transaction(update);
    }
    if update.account_keys.contains(&pumpfun_amm) {
        return parse_pumpfun_amm_transaction(update);
    }
    if update.account_keys.contains(&pumpfun) {
        return parse_pumpfun_transaction(update);
    }
    if update.account_keys.contains(&raydium) {
        return parse_raydium_transaction(update);
    }
    if update.account_keys.contains(&orca) {
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

        if result.is_some() {
            return result;
        }
    }

    None
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

    // Orca swap discriminator
    const ORCA_SWAP: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

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

    // Calculate actual amounts from native and token balance changes
    // SOL is native balance, tokens from token_balances
    let (sol_amount, token_amount) = if is_buy {
        // BUY: Calculate SOL spent from native balance
        let sol_spent = calculate_native_balance_change(
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

        (sol_spent, tokens_received)
    } else {
        // SELL: Calculate SOL received from native balance
        let sol_received = calculate_native_balance_change(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &trader,
        )
        .unwrap_or(0);

        (sol_received, token_amount_param)
    };

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

    let token_decimals = get_token_decimals(&update.post_token_balances, &mint);

    // NOTE: Do NOT extract creator from swap instruction accounts!
    // account[1] is Fee Recipient, not the token creator.
    // Creator must come from:
    // 1. PoolCreated event (instruction_accounts[7] in CREATE)
    // 2. market-data creator_cache (populated from PoolCreated)
    // 3. On-chain bonding curve state (if cache miss)
    let creator: Option<Pubkey> = None;

    // Extract Token Program from post_token_balances (authoritative source).
    // This is more reliable than instruction_accounts position which can vary
    // depending on CPI calls, routing changes, or program updates.
    // PumpFun now supports both SPL Token AND Token-2022 mints.
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

    Some(ParsedDexEvent::Trade {
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
    })
}

// ============================================================================
// Pump.fun AMM (PumpSwap) Parsing
// ============================================================================

/// Build 14 swap pool_accounts from create_pool instruction accounts.
/// IDL order: pool[0], global_config[1], creator[2], base_mint[3], quote_mint[4],
/// lp_mint[5], user_base[6], user_quote[7], user_pool[8], pool_base_vault[9],
/// pool_quote_vault[10], system[11], token_2022[12], base_tp[13], quote_tp[14],
/// ata_program[15], event_authority[16], program[17].
fn build_pool_accounts_from_create_pool(accounts: &[Pubkey]) -> Option<Vec<Pubkey>> {
    if accounts.len() < 18 {
        return None;
    }
    // Resolve accounts: for top-level, accounts IS the list. For inner, accounts are indices.
    // We receive already-resolved Pubkeys in accounts from the caller.
    let get = |i: usize| accounts.get(i).copied();

    let pool = get(0)?;
    let global_config = get(1)?;
    let creator = get(2)?;
    let base_mint = get(3)?;
    let quote_mint = get(4)?;
    let pool_base_vault = get(9)?;
    let pool_quote_vault = get(10)?;
    let base_token_program = get(13)?;
    let quote_token_program = get(14)?;
    let event_authority = get(16)?;

    let fee_config = Pubkey::from_str("5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx").ok()?;
    let fee_program = Pubkey::from_str("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ").ok()?;
    let protocol_fee_recipient =
        Pubkey::from_str("JCRGumoE9Qi5BBgULTgdgTLjSgkCMSbF62ZZfGs84JeU").ok()?;

    let protocol_fee_recipient_ta =
        spl_associated_token_account::get_associated_token_address_with_program_id(
            &to_spl_pubkey(&protocol_fee_recipient),
            &to_spl_pubkey(&quote_mint),
            &to_spl_pubkey(&quote_token_program),
        );
    let protocol_fee_recipient_ta = Pubkey::new_from_array(protocol_fee_recipient_ta.to_bytes());
    let coin_creator_vault_ata =
        spl_associated_token_account::get_associated_token_address_with_program_id(
            &to_spl_pubkey(&creator),
            &to_spl_pubkey(&base_mint),
            &to_spl_pubkey(&base_token_program),
        );
    let coin_creator_vault_ata = Pubkey::new_from_array(coin_creator_vault_ata.to_bytes());

    let pool_accounts = vec![
        pool,
        global_config,
        base_mint,
        quote_mint,
        pool_base_vault,
        pool_quote_vault,
        protocol_fee_recipient,
        protocol_fee_recipient_ta,
        event_authority,
        coin_creator_vault_ata,
        creator,           // coin_creator_vault_authority
        Pubkey::default(), // global_volume_accumulator (not in create)
        fee_config,
        fee_program,
    ];
    Some(pool_accounts)
}

/// Try to parse create_pool from top-level or inner instructions.
fn try_parse_pumpfun_amm_create_pool(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    let pumpfun_amm = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).ok()?;

    // 1. Check top-level instruction
    if update.instruction_data.len() >= 8
        && update.instruction_data[0..8] == PUMPFUN_AMM_CREATE_POOL
        && update.instruction_accounts.len() >= 18
    {
        if let Some(pool_accounts) =
            build_pool_accounts_from_create_pool(&update.instruction_accounts)
        {
            let pool = update.instruction_accounts[0];
            let base_mint = update.instruction_accounts[3];
            let quote_mint = update.instruction_accounts[4];
            let creator = update.instruction_accounts[2];
            info!(
                pool = %pool,
                base_mint = %base_mint,
                "PumpSwap create_pool detected (top-level) - pool_accounts available"
            );
            return Some(ParsedDexEvent::PoolCreated {
                pool_address: pool,
                base_mint,
                quote_mint,
                dex: DexType::PumpFunAmm,
                initial_liquidity_lamports: 0,
                slot: update.slot,
                creator: Some(creator),
                pool_accounts: Some(pool_accounts),
            });
        }
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
        if let Some(pool_accounts) = build_pool_accounts_from_create_pool(&accounts) {
            let pool = accounts[0];
            let base_mint = accounts[3];
            let quote_mint = accounts[4];
            let creator = accounts[2];
            info!(
                pool = %pool,
                base_mint = %base_mint,
                "PumpSwap create_pool detected (inner/CPI) - pool_accounts available"
            );
            return Some(ParsedDexEvent::PoolCreated {
                pool_address: pool,
                base_mint,
                quote_mint,
                dex: DexType::PumpFunAmm,
                initial_liquidity_lamports: 0,
                slot: update.slot,
                creator: Some(creator),
                pool_accounts: Some(pool_accounts),
            });
        }
    }

    None
}

fn parse_pumpfun_amm_transaction(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    // P12 Option A: Try create_pool first (pool_accounts from Create-IX)
    if let Some(created) = try_parse_pumpfun_amm_create_pool(update) {
        return Some(created);
    }

    if update.instruction_data.len() < 24 {
        return None;
    }
    if update.instruction_accounts.len() < 21 {
        return None;
    }

    let disc = &update.instruction_data[0..8];
    let buy_disc = anchor_disc("buy_exact_quote_in");
    let sell_disc = anchor_disc("sell");

    let is_buy = if disc == buy_disc {
        true
    } else if disc == sell_disc {
        false
    } else {
        return None;
    };

    // Observed account order from on-chain PumpFun AMM swap TX:
    // BUY: 23 accounts (includes global_volume_accumulator + user_volume)
    // SELL: 21 accounts (no volume accumulators)
    // See src/solana/dex/pumpfun_amm.rs build_swap_ix_from_pool_accounts for reference.
    let pool_market = update.instruction_accounts[0];
    let user = update.instruction_accounts[1];
    let trader = update.account_keys.first().copied().unwrap_or(user);
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
    //       [19]=fee_config (pool-specific PDA), [20]=fee_program
    //
    // CRITICAL: Transaction fee_config accounts are pool-specific PDAs (AMM-owned).
    // We must use global constants instead:
    //   - PUMPFUN_AMM_FEE_CONFIG = 5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx
    //   - PUMPFUN_AMM_FEE_PROGRAM_ID = pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ
    let global_volume_accumulator = if is_buy {
        update.instruction_accounts[16]
    } else {
        Pubkey::default()
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
        // BUY: amount_in is SOL (from native balance), need to calculate tokens received
        let tokens_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &base_mint,
        )
        .unwrap_or(0);

        // Extract SOL spent from native balance (user account decreased)
        let sol_spent = calculate_native_balance_change(
            &update.account_keys,
            &update.pre_balances,
            &update.post_balances,
            &trader,
        )
        .unwrap_or(amount_in); // Fallback to instruction amount

        (sol_spent, tokens_received)
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
    // For SELL, global_volume_accumulator will be Pubkey::default() (not needed)
    let pool_accounts = vec![
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
        global_volume_accumulator, // [11] - v1 format: for BUY this is real, for SELL it's default
        fee_config,                // [12]
        fee_program,               // [13]
    ];

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

    let amount_in = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let _min_out = u64::from_le_bytes(update.instruction_data[16..24].try_into().ok()?);

    // Determine direction from balance changes
    // Find which token increased (that's what user received)
    let base_mint = update
        .post_token_balances
        .iter()
        .find(|post| {
            let post_amt: u64 = post.ui_token_amount.amount.parse().unwrap_or(0);
            let pre_amt: u64 = update
                .pre_token_balances
                .iter()
                .find(|pre| pre.mint == post.mint && pre.account_index == post.account_index)
                .and_then(|pre| pre.ui_token_amount.amount.parse().ok())
                .unwrap_or(0);
            post_amt > pre_amt && post.mint != "So11111111111111111111111111111111111111112"
        })
        .and_then(|b| Pubkey::from_str(&b.mint).ok())
        .unwrap_or_default();

    let is_buy = base_mint != Pubkey::default();

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
        "Meteora DLMM swap detected"
    );

    let token_decimals = get_token_decimals(&update.post_token_balances, &base_mint);

    // Extract actual quote_mint from token balance changes.
    // In a swap, exactly 2 mints are involved: base and quote.
    // For non-SOL pairs (e.g., TOKEN/USDC), this correctly identifies the
    // quote_mint so arb-strategy can filter them out.
    let quote_mint = extract_quote_mint(&update.post_token_balances, &base_mint);

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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solana::geyser_listener::GeyserTransactionUpdate;

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
                let accounts = pool_accounts.as_ref().expect("pool_accounts");
                assert_eq!(accounts.len(), 14);
                assert_eq!(accounts[0], pool);
                assert_eq!(accounts[1], global_config);
                assert_eq!(accounts[2], base_mint);
                assert_eq!(accounts[3], quote_mint);
            }
            _ => panic!("expected PoolCreated, got {:?}", event),
        }
    }
}
