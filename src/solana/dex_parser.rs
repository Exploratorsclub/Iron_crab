//! DEX Transaction/Account Parsing for MarketEvents
//!
//! Public parsing functions for market-data service to convert raw Geyser
//! updates into structured MarketEvents (PoolCreated, Trade, etc.)
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.1
//! Supports: Raydium AMM V4, Orca Whirlpool, PumpFun

use crate::solana::geyser_listener::{GeyserAccountUpdate, GeyserTransactionUpdate};
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tracing::{debug, info, trace};

// ============================================================================
// Program IDs
// ============================================================================

pub const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
pub const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

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
    },
    /// Trade/Swap observed
    Trade {
        pool_address: Pubkey,
        mint: Pubkey,
        trader: Pubkey,
        is_buy: bool,
        sol_amount: u64,
        token_amount: u64,
        signature: String,
        slot: u64,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DexType {
    RaydiumAmmV4,
    OrcaWhirlpool,
    PumpFun,
}

impl std::fmt::Display for DexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DexType::RaydiumAmmV4 => write!(f, "raydium"),
            DexType::OrcaWhirlpool => write!(f, "orca"),
            DexType::PumpFun => write!(f, "pumpfun"),
        }
    }
}

// ============================================================================
// Instruction Discriminators
// ============================================================================

// PumpFun discriminators (Anchor IDL)
const PUMPFUN_CREATE: [u8; 8] = [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];
const PUMPFUN_BUY: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
const PUMPFUN_SELL: [u8; 8] = [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];

// Raydium AMM V4 discriminators
const RAYDIUM_SWAP_BASE_IN: u8 = 9;
const RAYDIUM_SWAP_BASE_OUT: u8 = 11;
const RAYDIUM_INITIALIZE2: u8 = 1;

// ============================================================================
// Main Parsing Functions (Public API)
// ============================================================================

/// Parse an account update into a DEX event (pool creation/update)
/// Used for Raydium and Orca where pool state lives in accounts
pub fn parse_account_update(update: &GeyserAccountUpdate) -> Option<ParsedDexEvent> {
    let owner_str = update.owner.to_string();

    match owner_str.as_str() {
        RAYDIUM_AMM_V4 => parse_raydium_account(update),
        ORCA_WHIRLPOOL => parse_orca_account(update),
        PUMPFUN_PROGRAM => {
            // PumpFun uses TX-based discovery, account updates are bonding curve state
            // We could parse price updates here, but PoolCreated comes from TX
            None
        }
        _ => None,
    }
}

/// Parse a transaction update into a DEX event (pool creation, swap)
/// Used for all DEXes for swap detection, and PumpFun for pool creation
pub fn parse_transaction_update(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    // Identify DEX by checking account keys
    let raydium = Pubkey::from_str(RAYDIUM_AMM_V4).ok()?;
    let orca = Pubkey::from_str(ORCA_WHIRLPOOL).ok()?;
    let pumpfun = Pubkey::from_str(PUMPFUN_PROGRAM).ok()?;

    if update.account_keys.contains(&pumpfun) {
        return parse_pumpfun_transaction(update);
    }
    if update.account_keys.contains(&raydium) {
        return parse_raydium_transaction(update);
    }
    if update.account_keys.contains(&orca) {
        return parse_orca_transaction(update);
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
    let trader = update.instruction_accounts[16];

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

    // We need the mint - for now use placeholder, will be enriched by momentum-bot
    let mint = Pubkey::default();

    debug!(
        pool = %pool_address,
        trader = %trader,
        is_buy = is_buy,
        amount = amount_in,
        sig = %update.signature,
        "Raydium swap detected"
    );

    Some(ParsedDexEvent::Trade {
        pool_address,
        mint,
        trader,
        is_buy,
        sol_amount: if is_buy { amount_in } else { 0 },
        token_amount: if !is_buy { amount_in } else { 0 },
        signature: update.signature.clone(),
        slot: update.slot,
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
    })
}

fn parse_orca_transaction(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
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
    let trader = update.instruction_accounts[3];

    // Parse amounts
    // Layout: discriminator(8) + amount(8) + other_amount_threshold(8) + sqrt_price_limit(16) + ...
    if update.instruction_data.len() < 25 {
        return None;
    }

    let amount = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let a_to_b = update.instruction_data[24] == 1; // direction flag

    let is_buy = !a_to_b; // a_to_b=true means selling token A

    debug!(
        pool = %pool_address,
        trader = %trader,
        is_buy = is_buy,
        amount = amount,
        sig = %update.signature,
        "Orca swap detected"
    );

    Some(ParsedDexEvent::Trade {
        pool_address,
        mint: Pubkey::default(),
        trader,
        is_buy,
        sol_amount: if is_buy { amount } else { 0 },
        token_amount: if !is_buy { amount } else { 0 },
        signature: update.signature.clone(),
        slot: update.slot,
    })
}

// ============================================================================
// PumpFun Parsing
// ============================================================================

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

    if update.instruction_accounts.len() < 8 {
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
    let trader = update.instruction_accounts[6];

    // Parse amounts from instruction data
    // BUY layout: discriminator(8) + amount(8) + max_sol_cost(8)
    // SELL layout: discriminator(8) + amount(8) + min_sol_output(8)
    if update.instruction_data.len() < 24 {
        return None;
    }

    let token_amount = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let sol_amount = u64::from_le_bytes(update.instruction_data[16..24].try_into().ok()?);

    debug!(
        pool = %bonding_curve,
        mint = %mint,
        trader = %trader,
        is_buy = is_buy,
        sol = sol_amount,
        tokens = token_amount,
        sig = %update.signature,
        "PumpFun swap detected"
    );

    Some(ParsedDexEvent::Trade {
        pool_address: bonding_curve,
        mint,
        trader,
        is_buy,
        sol_amount,
        token_amount,
        signature: update.signature.clone(),
        slot: update.slot,
    })
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
            } => {
                let liq_sol =
                    Decimal::from(*initial_liquidity_lamports) / Decimal::from(1_000_000_000u64);
                MarketEventKind::PoolCreated {
                    pool_address: pool_address.to_string(),
                    base_mint: base_mint.to_string(),
                    quote_mint: quote_mint.to_string(),
                    dex: dex.to_string(),
                    initial_liquidity_sol: Some(liq_sol),
                }
            }
            ParsedDexEvent::Trade {
                pool_address,
                mint,
                trader,
                is_buy,
                sol_amount,
                token_amount,
                signature,
                ..
            } => MarketEventKind::Trade {
                pool_address: pool_address.to_string(),
                mint: mint.to_string(),
                trader: trader.to_string(),
                is_buy: *is_buy,
                sol_amount: *sol_amount,
                token_amount: *token_amount,
                signature: Some(signature.clone()),
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
        }
    }

    /// Get slot from event
    pub fn slot(&self) -> u64 {
        match self {
            ParsedDexEvent::PoolCreated { slot, .. } => *slot,
            ParsedDexEvent::Trade { slot, .. } => *slot,
            ParsedDexEvent::LiquidityRemoved { slot, .. } => *slot,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dex_type_display() {
        assert_eq!(DexType::RaydiumAmmV4.to_string(), "raydium");
        assert_eq!(DexType::OrcaWhirlpool.to_string(), "orca");
        assert_eq!(DexType::PumpFun.to_string(), "pumpfun");
    }
}
