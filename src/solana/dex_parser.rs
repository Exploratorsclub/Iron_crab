//! DEX Transaction/Account Parsing for MarketEvents
//!
//! Public parsing functions for market-data service to convert raw Geyser
//! updates into structured MarketEvents (PoolCreated, Trade, etc.)
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.1
//! Supports: Raydium AMM V4, Orca Whirlpool, PumpFun

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
        trader: Pubkey,
        dex: DexType,
        is_buy: bool,
        sol_amount: u64,
        token_amount: u64,
        signature: String,
        slot: u64,
        /// Optional static pool account list for deterministic intent building.
        /// Order is DEX-specific.
        pool_accounts: Option<Vec<Pubkey>>,
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

    // Get source and destination token accounts to determine mints
    let user_source = update.instruction_accounts.get(14).copied()?;
    let user_destination = update.instruction_accounts.get(15).copied()?;

    // Extract mints from token balances
    let base_mint = if is_buy {
        // BUY: destination receives base tokens
        update
            .post_token_balances
            .iter()
            .find(|b| b.account_index == 15)
            .and_then(|b| Pubkey::from_str(&b.mint).ok())
            .unwrap_or_default()
    } else {
        // SELL: source spends base tokens
        update
            .pre_token_balances
            .iter()
            .find(|b| b.account_index == 14)
            .and_then(|b| Pubkey::from_str(&b.mint).ok())
            .unwrap_or_default()
    };

    let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
        .unwrap_or_default(); // SOL

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
        // SELL: User pays tokens, receives SOL
        let sol_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &quote_mint,
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

    Some(ParsedDexEvent::Trade {
        pool_address,
        mint: base_mint,
        trader,
        dex: DexType::RaydiumAmmV4,
        is_buy,
        sol_amount,
        token_amount,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: None,
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

    // Extract mints from token balances (similar to Raydium)
    // Account indices vary, use mint matching instead
    let base_mint = if is_buy {
        // BUY: Find token account with increasing balance
        update
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
                    .find(|post| post.mint == pre.mint && post.account_index == pre.account_index)
                    .and_then(|post| post.ui_token_amount.amount.parse().ok())
                    .unwrap_or(0);
                pre_amt > post_amt && pre.mint != "So11111111111111111111111111111111111111112"
            })
            .and_then(|b| Pubkey::from_str(&b.mint).ok())
            .unwrap_or_default()
    };

    let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
        .unwrap_or_default(); // SOL

    // Calculate actual amounts from token balance changes
    let (sol_amount, token_amount) = if is_buy {
        let tokens_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &base_mint,
        )
        .unwrap_or(0);
        (amount, tokens_received)
    } else {
        let sol_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &quote_mint,
        )
        .unwrap_or(0);
        (sol_received, amount)
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

    Some(ParsedDexEvent::Trade {
        pool_address,
        mint: base_mint,
        trader,
        dex: DexType::OrcaWhirlpool,
        is_buy,
        sol_amount,
        token_amount,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: None,
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
    let trader = update.instruction_accounts[6];

    // Parse amounts from instruction data
    // BUY layout: discriminator(8) + token_amount(8) + max_sol_cost(8)
    // SELL layout: discriminator(8) + token_amount(8) + min_sol_output(8)
    if update.instruction_data.len() < 24 {
        return None;
    }

    let token_amount_param = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let _sol_limit = u64::from_le_bytes(update.instruction_data[16..24].try_into().ok()?);

    let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
        .unwrap_or_default();

    // Calculate actual amounts from token balance changes
    let (sol_amount, token_amount) = if is_buy {
        // BUY: Calculate SOL spent and tokens received
        let sol_spent = calculate_token_balance_change(
            &update.post_token_balances,  // Note: reversed for spent
            &update.pre_token_balances,
            &quote_mint,
        )
        .unwrap_or(0);  // How much SOL was paid
        
        let tokens_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &mint,
        )
        .unwrap_or(0);
        
        (sol_spent, tokens_received)
    } else {
        // SELL: Calculate tokens sold and SOL received
        let sol_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &quote_mint,
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

    Some(ParsedDexEvent::Trade {
        pool_address: bonding_curve,
        mint,
        trader,
        dex: DexType::PumpFun,
        is_buy,
        sol_amount,
        token_amount,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: None,
    })
}

// ============================================================================
// Pump.fun AMM (PumpSwap) Parsing
// ============================================================================

fn parse_pumpfun_amm_transaction(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    if update.instruction_data.len() < 24 {
        return None;
    }
    if update.instruction_accounts.len() != 23 {
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

    // Observed account order (see src/solana/dex/pumpfun_amm.rs)
    let pool_market = update.instruction_accounts[0];
    let user = update.instruction_accounts[1];
    let global_config = update.instruction_accounts[2];
    let base_mint = update.instruction_accounts[3];
    let quote_mint = update.instruction_accounts[4];
    let pool_base_vault = update.instruction_accounts[7];
    let pool_quote_vault = update.instruction_accounts[8];
    let protocol_fee_recipient = update.instruction_accounts[9];
    let protocol_fee_recipient_ta = update.instruction_accounts[10];
    let event_authority = update.instruction_accounts[15];
    let coin_creator_vault_ata = update.instruction_accounts[17];
    let coin_creator_vault_authority = update.instruction_accounts[18];
    let global_volume_accumulator = update.instruction_accounts[19];
    let fee_config = update.instruction_accounts[21];
    let fee_program = update.instruction_accounts[22];

    let amount_in = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let min_out = u64::from_le_bytes(update.instruction_data[16..24].try_into().ok()?);

    // Calculate actual amount_out from token balance changes
    // For BUY: user receives base tokens (token_amount)
    // For SELL: user receives quote tokens (sol_amount)
    let (sol_amount, token_amount) = if is_buy {
        // BUY: amount_in is SOL, need to calculate tokens received
        let tokens_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &base_mint,
        ).unwrap_or(0);
        (amount_in, tokens_received)
    } else {
        // SELL: amount_in is tokens, need to calculate SOL received
        let sol_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &quote_mint,
        ).unwrap_or(0);
        (sol_received, amount_in)
    };

    // Pool static accounts order (v1) for intent resources.accounts
    let pool_accounts = vec![
        pool_market,
        global_config,
        base_mint,
        quote_mint,
        pool_base_vault,
        pool_quote_vault,
        protocol_fee_recipient,
        protocol_fee_recipient_ta,
        event_authority,
        coin_creator_vault_ata,
        coin_creator_vault_authority,
        global_volume_accumulator,
        fee_config,
        fee_program,
    ];

    debug!(
        pool = %pool_market,
        mint = %base_mint,
        trader = %user,
        is_buy = is_buy,
        amount_in = amount_in,
        sol_amount = sol_amount,
        token_amount = token_amount,
        sig = %update.signature,
        "Pump.fun AMM swap detected"
    );

    Some(ParsedDexEvent::Trade {
        pool_address: pool_market,
        mint: base_mint,
        trader: user,
        dex: DexType::PumpFunAmm,
        is_buy,
        sol_amount,
        token_amount,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: Some(pool_accounts),
    })
}

/// Calculate token balance change for a specific mint
/// Returns positive value for increase (received), negative for decrease (sent)
fn calculate_token_balance_change(
    pre_balances: &[crate::solana::geyser_listener::TokenBalance],
    post_balances: &[crate::solana::geyser_listener::TokenBalance],
    mint: &Pubkey,
) -> Option<u64> {
    let mint_str = mint.to_string();
    
    // Find pre and post balance for this mint
    let pre_balance = pre_balances
        .iter()
        .find(|b| b.mint == mint_str)?
        .ui_token_amount
        .amount
        .parse::<u64>()
        .ok()?;
    
    let post_balance = post_balances
        .iter()
        .find(|b| b.mint == mint_str)?
        .ui_token_amount
        .amount
        .parse::<u64>()
        .ok()?;
    
    // Return absolute change (received amount)
    Some(post_balance.saturating_sub(pre_balance))
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

// ============================================================================
// Meteora DLMM Parsing
// ============================================================================

fn parse_meteora_transaction(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    if update.instruction_data.len() < 8 {
        return None;
    }

    let disc = &update.instruction_data[0..8];
    if disc != METEORA_SWAP {
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
        return None;
    }

    let pool_address = update.instruction_accounts[0];
    let trader = update.instruction_accounts[5];

    // Parse amounts: discriminator(8) + amount_in(8) + min_out(8)
    if update.instruction_data.len() < 24 {
        return None;
    }

    let amount_in = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let _min_out = u64::from_le_bytes(update.instruction_data[16..24].try_into().ok()?);

    // Determine direction from balance changes
    let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
        .unwrap_or_default();

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
        let sol_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &quote_mint,
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

    Some(ParsedDexEvent::Trade {
        pool_address,
        mint: base_mint,
        trader,
        dex: DexType::MeteoraDlmm,
        is_buy,
        sol_amount,
        token_amount,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: None,
    })
}

// ============================================================================
// Raydium CPMM Parsing
// ============================================================================

fn parse_raydium_cpmm_transaction(update: &GeyserTransactionUpdate) -> Option<ParsedDexEvent> {
    if update.instruction_data.len() < 8 {
        return None;
    }

    let disc = &update.instruction_data[0..8];
    if disc != RAYDIUM_CPMM_SWAP {
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
        return None;
    }

    let pool_address = update.instruction_accounts[3];
    let trader = update.instruction_accounts[0];

    // Parse amounts: discriminator(8) + amount_in(8) + min_out(8)
    if update.instruction_data.len() < 24 {
        return None;
    }

    let amount_in = u64::from_le_bytes(update.instruction_data[8..16].try_into().ok()?);
    let _min_out = u64::from_le_bytes(update.instruction_data[16..24].try_into().ok()?);

    // Extract mints from accounts
    let vault_0_mint = update.instruction_accounts.get(10).copied()?;
    let vault_1_mint = update.instruction_accounts.get(11).copied()?;

    let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
        .unwrap_or_default();

    // Determine which is base (non-SOL) and which is quote (SOL)
    let (base_mint, is_buy) = if vault_0_mint == quote_mint {
        (vault_1_mint, true) // Vault 0 = SOL, so buying vault 1 token
    } else if vault_1_mint == quote_mint {
        (vault_0_mint, false) // Vault 1 = SOL, so selling vault 0 token
    } else {
        // Non-SOL pair, use first vault as base
        (vault_0_mint, true)
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
        let sol_received = calculate_token_balance_change(
            &update.pre_token_balances,
            &update.post_token_balances,
            &quote_mint,
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

    Some(ParsedDexEvent::Trade {
        pool_address,
        mint: base_mint,
        trader,
        dex: DexType::RaydiumCpmm,
        is_buy,
        sol_amount,
        token_amount,
        signature: update.signature.clone(),
        slot: update.slot,
        pool_accounts: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dex_type_display() {
        assert_eq!(DexType::RaydiumAmmV4.to_string(), "raydium");
        assert_eq!(DexType::OrcaWhirlpool.to_string(), "orca");
        assert_eq!(DexType::PumpFun.to_string(), "pumpfun");
        assert_eq!(DexType::PumpFunAmm.to_string(), "pump_amm");
    }
}
