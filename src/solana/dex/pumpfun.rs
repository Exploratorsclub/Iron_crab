//! Pump.fun DEX Integration
//! Bonding curve-based token swaps
//! Program: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::{Dex, Quote};
use crate::execution::live_pool_cache::{CachedPoolState, SharedLivePoolCache};
use crate::solana::rpc::SolanaRpc;

fn sdk_pubkey_to_spl(pk: &Pubkey) -> spl_token::solana_program::pubkey::Pubkey {
    spl_token::solana_program::pubkey::Pubkey::new_from_array(pk.to_bytes())
}

fn prog_pubkey_to_sdk(pk: spl_token::solana_program::pubkey::Pubkey) -> Pubkey {
    Pubkey::new_from_array(pk.to_bytes())
}

fn prog_ix_to_sdk(ix: spl_token::solana_program::instruction::Instruction) -> Instruction {
    Instruction {
        program_id: prog_pubkey_to_sdk(ix.program_id),
        accounts: ix
            .accounts
            .into_iter()
            .map(|m| AccountMeta {
                pubkey: prog_pubkey_to_sdk(m.pubkey),
                is_signer: m.is_signer,
                is_writable: m.is_writable,
            })
            .collect(),
        data: ix.data,
    }
}

/// System Program ID
const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";

/// Pump.fun program ID
pub const PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

/// Pump.fun global config account (for fee structure)
pub const PUMPFUN_GLOBAL: &str = "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf";

/// Pump.fun fee recipient
pub const PUMPFUN_FEE_RECIPIENT: &str = "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM";

/// Pump.fun event authority
pub const PUMPFUN_EVENT_AUTHORITY: &str = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1";

/// Pump.fun fee program (new accounts added to protocol)
pub const PUMPFUN_FEE_PROGRAM: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";

/// One of Pump.fun's documented buyback/fee recipient accounts (Apr 2026 breaking fee-recipient upgrade).
/// Must be appended **after** `bonding_curve_v2` on bonding-curve BUY/SELL; must be writable on-chain.
/// See: pump-public-docs `BREAKING_FEE_RECIPIENT.md`.
pub const PUMPFUN_BUYBACK_FEE_RECIPIENT: &str = "5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD";

/// Borsh serialization of IDL `OptionBool` with inner `false` (one bool field → 1 byte).
/// Pump.fun `buy` / `buy_exact_sol_in` require this as the third instruction-data argument.
const TRACK_VOLUME_OPTION_BOOL_FALSE: u8 = 0;

/// Fee config seed constant (32 bytes from IDL)
pub const FEE_CONFIG_SEED: [u8; 32] = [
    1, 86, 224, 246, 147, 102, 90, 207, 68, 219, 21, 104, 191, 23, 91, 170, 81, 137, 203, 151, 245,
    210, 255, 59, 101, 93, 43, 182, 253, 109, 24, 176,
];

/// Bonding curve account data layout
/// Note: Layout has been updated in recent Pump.fun version
/// Layout (updated):
/// - 0-7: discriminator (8 bytes)
/// - 8-15: virtual_token_reserves (u64)
/// - 16-23: virtual_sol_reserves (u64)  
/// - 24-31: real_token_reserves (u64)
/// - 32-39: real_sol_reserves (u64)
/// - 40-47: token_total_supply (u64)
/// - 48: complete (bool)
/// - 49-80: creator (Pubkey, 32 bytes)
#[derive(Debug, Clone)]
pub struct BondingCurveState {
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub token_total_supply: u64,
    pub complete: bool,
    pub creator: Pubkey,
    /// Cashback enabled (byte 82 in bonding curve data). Required for SELL account layout since Feb 2026.
    pub cashback_enabled: bool,
}

impl BondingCurveState {
    /// Parse bonding curve account data (new layout)
    /// Layout:
    /// - 0-7: discriminator (8 bytes)
    /// - 8-15: virtual_token_reserves (u64)
    /// - 16-23: virtual_sol_reserves (u64)
    /// - 24-31: real_token_reserves (u64)
    /// - 32-39: real_sol_reserves (u64)
    /// - 40-47: token_total_supply (u64)
    /// - 48: complete (bool)
    /// - 49-80: creator (Pubkey, 32 bytes)
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 81 {
            return Err(anyhow!(
                "bonding curve data too short: {} bytes (need 81)",
                data.len()
            ));
        }

        Ok(Self {
            virtual_token_reserves: u64::from_le_bytes(data[8..16].try_into()?),
            virtual_sol_reserves: u64::from_le_bytes(data[16..24].try_into()?),
            real_token_reserves: u64::from_le_bytes(data[24..32].try_into()?),
            real_sol_reserves: u64::from_le_bytes(data[32..40].try_into()?),
            token_total_supply: u64::from_le_bytes(data[40..48].try_into()?),
            complete: data[48] != 0,
            creator: Pubkey::new_from_array(data[49..81].try_into()?),
            cashback_enabled: if data.len() > 82 {
                data[82] != 0
            } else {
                false
            },
        })
    }

    /// Calculate output amount for constant product (x * y = k)
    /// Formula: amount_out = (amount_in * out_reserve) / (in_reserve + amount_in)
    pub fn calculate_output(&self, amount_in: u64, buy_token: bool) -> u64 {
        if buy_token {
            // Buy token with SOL: SOL in, Token out
            let sol_in = amount_in as u128;
            let token_reserve = self.virtual_token_reserves as u128;
            let sol_reserve = self.virtual_sol_reserves as u128;

            if sol_reserve == 0 {
                return 0;
            }

            // amount_out = (amount_in * token_reserve) / (sol_reserve + amount_in)
            let numerator = sol_in.saturating_mul(token_reserve);
            let denominator = sol_reserve.saturating_add(sol_in);

            (numerator / denominator.max(1)) as u64
        } else {
            // Sell token for SOL: Token in, SOL out
            let token_in = amount_in as u128;
            let sol_reserve = self.virtual_sol_reserves as u128;
            let token_reserve = self.virtual_token_reserves as u128;

            if token_reserve == 0 {
                return 0;
            }

            // amount_out = (amount_in * sol_reserve) / (token_reserve + amount_in)
            let numerator = token_in.saturating_mul(sol_reserve);
            let denominator = token_reserve.saturating_add(token_in);

            (numerator / denominator.max(1)) as u64
        }
    }
}

/// Pump.fun DEX connector
pub struct PumpFunDex {
    rpc: Arc<SolanaRpc>,
    program_id: Pubkey,
    global: Pubkey,
    fee_recipient: Pubkey,
    event_authority: Pubkey,
    /// User authority (wallet)
    user_authority: Option<Pubkey>,
    /// Cached creators for bonding curves (populated from LivePoolCache)
    /// Key: token_mint, Value: creator pubkey
    /// This eliminates the need for RPC calls to fetch creator during build_swap_ix
    cached_creators: DashMap<Pubkey, Pubkey>,
    /// Shared LivePoolCache for GEYSER-FIRST quote + creator resolution (PR2)
    live_pool_cache: Option<SharedLivePoolCache>,
}

impl PumpFunDex {
    pub fn new(rpc: Arc<SolanaRpc>, live_pool_cache: Option<SharedLivePoolCache>) -> Result<Self> {
        Ok(Self {
            rpc,
            program_id: Pubkey::from_str(PUMPFUN_PROGRAM_ID)?,
            global: Pubkey::from_str(PUMPFUN_GLOBAL)?,
            fee_recipient: Pubkey::from_str(PUMPFUN_FEE_RECIPIENT)?,
            event_authority: Pubkey::from_str(PUMPFUN_EVENT_AUTHORITY)?,
            user_authority: None,
            cached_creators: DashMap::new(),
            live_pool_cache,
        })
    }

    pub fn set_user_authority(&mut self, authority: Pubkey) {
        self.user_authority = Some(authority);
    }

    /// Cache a creator for a token mint (called from LivePoolCache/cross_dex_handler)
    /// This eliminates the need for RPC calls during build_swap_ix
    pub fn cache_creator(&self, token_mint: Pubkey, creator: Pubkey) {
        self.cached_creators.insert(token_mint, creator);
    }

    /// Get cached creator for a token mint
    pub fn get_cached_creator(&self, token_mint: &Pubkey) -> Option<Pubkey> {
        self.cached_creators.get(token_mint).map(|r| *r)
    }

    /// Get bonding curve state from LivePoolCache (GEYSER-FIRST, no RPC).
    /// Returns a BondingCurveState if the bonding curve is cached.
    fn get_bonding_curve_from_cache(&self, bonding_curve: &Pubkey) -> Option<BondingCurveState> {
        let cache = self.live_pool_cache.as_ref()?;
        let cached_state = cache.get(bonding_curve)?;
        if let CachedPoolState::PumpFun(state) = cached_state {
            Some(BondingCurveState {
                virtual_token_reserves: state.virtual_token_reserves,
                virtual_sol_reserves: state.virtual_sol_reserves,
                real_token_reserves: state.real_token_reserves,
                real_sol_reserves: state.real_sol_reserves,
                token_total_supply: 1_000_000_000_000_000, // PumpFun always 1B * 10^6
                complete: state.complete,
                creator: state.creator,
                cashback_enabled: state.cashback_enabled,
            })
        } else {
            None
        }
    }

    /// Get creator from LivePoolCache (GEYSER-FIRST, no RPC).
    fn get_creator_from_cache(&self, bonding_curve: &Pubkey) -> Option<Pubkey> {
        self.live_pool_cache
            .as_ref()?
            .get_pumpfun_creator(bonding_curve)
    }

    /// Derive bonding curve PDA for a token mint
    pub fn derive_bonding_curve(&self, token_mint: &Pubkey) -> (Pubkey, u8) {
        Self::derive_bonding_curve_static(token_mint)
    }

    /// Static version of derive_bonding_curve (can be called without instance)
    /// Useful for cross_dex_handler to derive bonding curve before injecting creator
    pub fn derive_bonding_curve_static(token_mint: &Pubkey) -> (Pubkey, u8) {
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).expect("valid pumpfun program id");
        Pubkey::find_program_address(&[b"bonding-curve", token_mint.as_ref()], &program_id)
    }

    /// Derive bonding curve v2 PDA (required since Feb 2026 cashback upgrade).
    /// In BUY/SELL account lists it appears before the buyback fee recipient (Apr 2026 layout).
    pub fn derive_bonding_curve_v2(token_mint: &Pubkey) -> (Pubkey, u8) {
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).expect("valid pumpfun program id");
        Pubkey::find_program_address(&[b"bonding-curve-v2", token_mint.as_ref()], &program_id)
    }

    /// Derive associated bonding curve token account (holds real token reserves).
    /// This is simply the ATA of the bonding curve PDA for the token mint.
    pub fn derive_associated_bonding_curve(
        &self,
        bonding_curve: &Pubkey,
        token_mint: &Pubkey,
        token_program: &Pubkey,
    ) -> (Pubkey, u8) {
        let bonding_curve_spl = sdk_pubkey_to_spl(bonding_curve);
        let token_mint_spl = sdk_pubkey_to_spl(token_mint);
        let token_program_spl = sdk_pubkey_to_spl(token_program);

        let ata_spl = spl_associated_token_account::get_associated_token_address_with_program_id(
            &bonding_curve_spl,
            &token_mint_spl,
            &token_program_spl,
        );

        (Pubkey::new_from_array(ata_spl.to_bytes()), 0)
    }

    /// Returns the token program for a pump.fun mint.
    ///
    /// If `override_program` is provided (from intent.resources.token_program via Geyser),
    /// use that. Otherwise, defaults to SPL Token (legacy behavior).
    ///
    /// The override MUST come from the Geyser-observed mint account owner.
    /// NO RPC calls in the hot path!
    fn token_program_for_mint(
        &self,
        _token_mint: &Pubkey,
        override_program: Option<&Pubkey>,
    ) -> Pubkey {
        if let Some(prog) = override_program {
            return *prog;
        }

        // Default to SPL Token if no override provided
        // This is a fallback - the correct token_program should come from Geyser via the intent
        Pubkey::new_from_array(spl_token::id().to_bytes())
    }

    /// Derive creator vault PDA (NEW in Pump.fun protocol update)
    /// Used for creator fee collection in buy/sell instructions
    pub fn derive_creator_vault(&self, creator: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[b"creator-vault", creator.as_ref()], &self.program_id)
    }

    /// Derive global volume accumulator PDA (NEW in Pump.fun protocol update)
    pub fn derive_global_volume_accumulator(&self) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[b"global_volume_accumulator"], &self.program_id)
    }

    /// Derive user volume accumulator PDA (NEW in Pump.fun protocol update)
    pub fn derive_user_volume_accumulator(&self, user: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[b"user_volume_accumulator", user.as_ref()],
            &self.program_id,
        )
    }

    /// Derive fee config PDA from fee program (NEW in Pump.fun protocol update)
    pub fn derive_fee_config() -> (Pubkey, u8) {
        let fee_program = Pubkey::from_str(PUMPFUN_FEE_PROGRAM).unwrap();
        Pubkey::find_program_address(&[b"fee_config", &FEE_CONFIG_SEED], &fee_program)
    }

    /// Fetch bonding curve state from chain
    pub async fn fetch_bonding_curve(&self, bonding_curve: &Pubkey) -> Result<BondingCurveState> {
        let account = self.rpc.get_account_retry(bonding_curve).await?;
        BondingCurveState::parse(&account.data)
    }

    /// Fetch bonding curve with fast timeout for sniping (no RPC retries)
    /// Returns None if account doesn't exist or timeout
    pub async fn fetch_bonding_curve_fast(
        &self,
        bonding_curve: &Pubkey,
    ) -> Option<BondingCurveState> {
        // Fast 2 second timeout - if RPC can't answer quickly, skip this opportunity
        let timeout = tokio::time::Duration::from_millis(2000);

        match tokio::time::timeout(timeout, self.rpc.get_account(bonding_curve)).await {
            Ok(Ok(account)) => BondingCurveState::parse(&account.data).ok(),
            _ => None,
        }
    }

    /// Returns the initial state of a Pump.fun bonding curve
    /// Used as fallback when RPC fails to index new pools fast enough
    /// This is the KNOWN initial state for ALL Pump.fun tokens at launch
    /// Note: creator must be provided as it cannot be inferred from token creation
    pub fn initial_bonding_curve_state(creator: Pubkey) -> BondingCurveState {
        BondingCurveState {
            virtual_token_reserves: 1_073_000_000_000_000,
            virtual_sol_reserves: 30_000_000_000,
            real_token_reserves: 793_100_000_000_000,
            real_sol_reserves: 30_000_000_000,
            token_total_supply: 1_000_000_000_000_000, // 1 billion tokens (6 decimals)
            complete: false,
            creator,
            cashback_enabled: false, // new tokens start without cashback
        }
    }

    /// Build buy instruction (SOL → Token)
    /// Instruction discriminator: 0x66063d1201daebea (8 bytes)
    ///
    /// UPDATED: 18 accounts — `bonding_curve_v2` then writable buyback fee recipient (Apr 2026).
    #[allow(clippy::too_many_arguments)]
    pub fn build_buy_ix(
        &self,
        token_mint: &Pubkey,
        bonding_curve: &Pubkey,
        associated_bonding_curve: &Pubkey,
        user_token_account: &Pubkey,
        creator: &Pubkey, // NEW: Creator pubkey for creator_vault derivation
        token_program: &Pubkey,
        amount_in: u64,    // SOL lamports
        max_sol_cost: u64, // Slippage protection
    ) -> Result<Instruction> {
        let user = self
            .user_authority
            .ok_or_else(|| anyhow!("user authority not set"))?;

        // Derive new PDAs required by updated protocol
        let (creator_vault, cv_bump) = self.derive_creator_vault(creator);
        let (global_volume_accumulator, gva_bump) = self.derive_global_volume_accumulator();
        let (user_volume_accumulator, uva_bump) = self.derive_user_volume_accumulator(&user);
        let (fee_config, fc_bump) = Self::derive_fee_config();
        let fee_program = Pubkey::from_str(PUMPFUN_FEE_PROGRAM)?;
        let (bonding_curve_v2, _) = Self::derive_bonding_curve_v2(token_mint);
        let buyback_fee_recipient = Pubkey::from_str(PUMPFUN_BUYBACK_FEE_RECIPIENT)?;

        // Log derived PDAs for debugging
        debug!(
            token_mint = %token_mint,
            creator = %creator,
            creator_vault = %creator_vault,
            cv_bump = cv_bump,
            global_volume_accumulator = %global_volume_accumulator,
            gva_bump = gva_bump,
            user = %user,
            user_volume_accumulator = %user_volume_accumulator,
            uva_bump = uva_bump,
            fee_config = %fee_config,
            fc_bump = fc_bump,
            "pump.fun BUY: derived PDAs for instruction"
        );

        // Instruction data: discriminator (8) + amount (8) + max_sol_cost (8) + track_volume OptionBool (1)
        let mut data = Vec::with_capacity(25);
        // Discriminator for "global:buy" = 66063d1201daebea
        data.extend_from_slice(&[0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea]);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&max_sol_cost.to_le_bytes());
        data.push(TRACK_VOLUME_OPTION_BOOL_FALSE);

        debug!(
            token_mint = %token_mint,
            track_volume = false,
            "pump.fun BUY: instruction data includes track_volume (OptionBool)"
        );

        Ok(Instruction {
            program_id: self.program_id,
            accounts: vec![
                // Accounts based on Solscan successful TX analysis (Dec 2024)
                // #1 (0): Global - readonly
                AccountMeta::new_readonly(self.global, false),
                // #2 (1): Fee Recipient - writable
                AccountMeta::new(self.fee_recipient, false),
                // #3 (2): Mint - readonly
                AccountMeta::new_readonly(*token_mint, false),
                // #4 (3): Bonding Curve - writable
                AccountMeta::new(*bonding_curve, false),
                // #5 (4): Associated Bonding Curve (token vault) - writable
                AccountMeta::new(*associated_bonding_curve, false),
                // #6 (5): Associated User (user's ATA) - writable
                AccountMeta::new(*user_token_account, false),
                // #7 (6): User (signer, fee payer) - writable, signer
                AccountMeta::new(user, true),
                // #8 (7): System Program - readonly
                AccountMeta::new_readonly(Pubkey::from_str(SYSTEM_PROGRAM_ID).unwrap(), false),
                // #9 (8): Token Program - readonly
                AccountMeta::new_readonly(*token_program, false),
                // #10 (9): Creator Vault - writable (receives creator fees)
                AccountMeta::new(creator_vault, false),
                // #11 (10): Event Authority - readonly
                AccountMeta::new_readonly(self.event_authority, false),
                // #12 (11): Program (Pump.fun) - readonly
                AccountMeta::new_readonly(self.program_id, false),
                // #13 (12): Global Volume Accumulator - readonly
                AccountMeta::new_readonly(global_volume_accumulator, false),
                // #14 (13): User Volume Accumulator - writable
                AccountMeta::new(user_volume_accumulator, false),
                // #15 (14): Fee Config - readonly
                AccountMeta::new_readonly(fee_config, false),
                // #16 (15): Fee Program (Pump Fees Program) - required for get_fees CPI
                AccountMeta::new_readonly(fee_program, false),
                // #17 (16): Bonding Curve V2 - readonly (required since Feb 2026 cashback upgrade)
                AccountMeta::new_readonly(bonding_curve_v2, false),
                // #18 (17): Buyback / fee recipient — writable, after bonding_curve_v2 (Apr 2026)
                AccountMeta::new(buyback_fee_recipient, false),
            ],
            data,
        })
    }

    /// Build buy_exact_sol_in instruction (Market Order: exact SOL in, min tokens out = 1)
    /// Instruction discriminator: [56, 252, 116, 8, 158, 223, 205, 95] (buy_exact_sol_in)
    /// Account layout: IDENTICAL to build_buy_ix (18 accounts)
    #[allow(clippy::too_many_arguments)]
    pub fn build_buy_exact_sol_ix(
        &self,
        token_mint: &Pubkey,
        bonding_curve: &Pubkey,
        associated_bonding_curve: &Pubkey,
        user_token_account: &Pubkey,
        creator: &Pubkey,
        token_program: &Pubkey,
        sol_amount: u64,
        min_tokens_out: u64,
    ) -> Result<Instruction> {
        let user = self
            .user_authority
            .ok_or_else(|| anyhow!("user authority not set"))?;

        let (creator_vault, _) = self.derive_creator_vault(creator);
        let (global_volume_accumulator, _) = self.derive_global_volume_accumulator();
        let (user_volume_accumulator, _) = self.derive_user_volume_accumulator(&user);
        let (fee_config, _) = Self::derive_fee_config();
        let fee_program = Pubkey::from_str(PUMPFUN_FEE_PROGRAM)?;
        let (bonding_curve_v2, _) = Self::derive_bonding_curve_v2(token_mint);
        let buyback_fee_recipient = Pubkey::from_str(PUMPFUN_BUYBACK_FEE_RECIPIENT)?;

        // Data: discriminator(8) + spendable_sol_in(8) + min_tokens_out(8) + track_volume OptionBool(1)
        let mut data = Vec::with_capacity(25);
        data.extend_from_slice(&[56, 252, 116, 8, 158, 223, 205, 95]);
        data.extend_from_slice(&sol_amount.to_le_bytes());
        data.extend_from_slice(&min_tokens_out.to_le_bytes());
        data.push(TRACK_VOLUME_OPTION_BOOL_FALSE);

        debug!(
            token_mint = %token_mint,
            track_volume = false,
            "pump.fun buy_exact_sol_in: instruction data includes track_volume (OptionBool)"
        );

        Ok(Instruction {
            program_id: self.program_id,
            accounts: vec![
                AccountMeta::new_readonly(self.global, false),
                AccountMeta::new(self.fee_recipient, false),
                AccountMeta::new_readonly(*token_mint, false),
                AccountMeta::new(*bonding_curve, false),
                AccountMeta::new(*associated_bonding_curve, false),
                AccountMeta::new(*user_token_account, false),
                AccountMeta::new(user, true),
                AccountMeta::new_readonly(Pubkey::from_str(SYSTEM_PROGRAM_ID).unwrap(), false),
                AccountMeta::new_readonly(*token_program, false),
                AccountMeta::new(creator_vault, false),
                AccountMeta::new_readonly(self.event_authority, false),
                AccountMeta::new_readonly(self.program_id, false),
                AccountMeta::new_readonly(global_volume_accumulator, false),
                AccountMeta::new(user_volume_accumulator, false),
                AccountMeta::new_readonly(fee_config, false),
                AccountMeta::new_readonly(fee_program, false),
                AccountMeta::new_readonly(bonding_curve_v2, false),
                AccountMeta::new(buyback_fee_recipient, false),
            ],
            data,
        })
    }

    /// Build sell instruction (Token → SOL)
    /// Instruction discriminator: 0x33e685a4017f83ad (8 bytes)
    ///
    /// Account count: 16 (non-cashback) or 17 (cashback). Last account is buyback fee recipient.
    #[allow(clippy::too_many_arguments)]
    pub fn build_sell_ix(
        &self,
        token_mint: &Pubkey,
        bonding_curve: &Pubkey,
        associated_bonding_curve: &Pubkey,
        user_token_account: &Pubkey,
        creator: &Pubkey, // NEW: Creator pubkey for creator_vault derivation
        token_program: &Pubkey,
        amount_in: u64,      // Token amount
        min_sol_output: u64, // Slippage protection
        cashback_enabled: bool,
    ) -> Result<Instruction> {
        let user = self
            .user_authority
            .ok_or_else(|| anyhow!("user authority not set"))?;

        // Derive new PDAs required by updated protocol
        let (creator_vault, _) = self.derive_creator_vault(creator);
        let (fee_config, _) = Self::derive_fee_config();
        let fee_program = Pubkey::from_str(PUMPFUN_FEE_PROGRAM)?;
        let (bonding_curve_v2, _) = Self::derive_bonding_curve_v2(token_mint);
        let buyback_fee_recipient = Pubkey::from_str(PUMPFUN_BUYBACK_FEE_RECIPIENT)?;
        let (user_volume_accumulator, _) = self.derive_user_volume_accumulator(&user);

        // Instruction data: discriminator (8 bytes) + amount (8 bytes) + min_output (8 bytes)
        let mut data = Vec::with_capacity(24);
        // Discriminator for "global:sell" = 33e685a4017f83ad
        data.extend_from_slice(&[0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad]);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_sol_output.to_le_bytes());

        let mut accounts = vec![
            // Original accounts (indices 0-7)
            AccountMeta::new_readonly(self.global, false), // 0: global
            AccountMeta::new(self.fee_recipient, false),   // 1: fee_recipient
            AccountMeta::new_readonly(*token_mint, false), // 2: mint
            AccountMeta::new(*bonding_curve, false),       // 3: bonding_curve
            AccountMeta::new(*associated_bonding_curve, false), // 4: associated_bonding_curve
            AccountMeta::new(*user_token_account, false),  // 5: associated_user (user's ATA)
            AccountMeta::new(user, true),                  // 6: user (signer, writable)
            AccountMeta::new_readonly(Pubkey::from_str(SYSTEM_PROGRAM_ID).unwrap(), false), // 7: system_program
            // NEW accounts (indices 8-13) - added in protocol update
            AccountMeta::new(creator_vault, false), // 8: creator_vault
            AccountMeta::new_readonly(*token_program, false), // 9: token_program
            AccountMeta::new_readonly(self.event_authority, false), // 10: event_authority
            AccountMeta::new_readonly(self.program_id, false), // 11: program
            AccountMeta::new_readonly(fee_config, false), // 12: fee_config
            AccountMeta::new_readonly(fee_program, false), // 13: fee_program
        ];
        if cashback_enabled {
            accounts.push(AccountMeta::new(user_volume_accumulator, false)); // 14: user_volume_accumulator
        }
        accounts.push(AccountMeta::new_readonly(bonding_curve_v2, false)); // before buyback recipient
        accounts.push(AccountMeta::new(buyback_fee_recipient, false)); // last: buyback fee recipient

        Ok(Instruction {
            program_id: self.program_id,
            accounts,
            data,
        })
    }

    /// Quote with fallback to initial bonding curve state for fresh token launches.
    /// Use this when the token was just discovered via Geyser CREATE instruction.
    ///
    /// The fallback is safe because:
    /// 1. Geyser CREATE means the bonding curve is being created in this block
    /// 2. The initial state of ALL Pump.fun bonding curves is deterministic
    /// 3. By the time our tx lands, the bonding curve will exist on-chain
    ///
    /// # Arguments
    /// * `fallback_creator` - Creator pubkey for fallback mode (from Geyser event)
    pub async fn quote_exact_in_with_fallback(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        use_fallback: bool,
        fallback_creator: Option<Pubkey>,
    ) -> Result<Option<Quote>> {
        let sol_mint = "So11111111111111111111111111111111111111112";

        // Determine direction: SOL→Token or Token→SOL
        let (token_mint_str, buy_token) = if input_mint == sol_mint {
            (output_mint, true)
        } else if output_mint == sol_mint {
            (input_mint, false)
        } else {
            return Ok(None); // Pump.fun only does SOL pairs
        };

        let token_mint = Pubkey::from_str(token_mint_str)?;
        let (bonding_curve, _bump) = self.derive_bonding_curve(&token_mint);

        info!(
            token_mint=%token_mint_str,
            bonding_curve=%bonding_curve,
            buy_token,
            use_fallback,
            "pump.fun: attempting to fetch bonding curve (with_fallback)"
        );

        // INSTANT INITIAL STATE - Zero RPC for fresh Geyser CREATE events!
        // Verified 2025-01-XX: Creator from TX accounts[7] MATCHES bonding curve creator.
        // The initial state is deterministic and known for all Pump.fun tokens.
        if let Some(creator) = fallback_creator.filter(|_| use_fallback && buy_token) {
            info!(
                token_mint=%token_mint_str,
                bonding_curve=%bonding_curve,
                creator=%creator,
                "pump.fun: ⚡ INSTANT INITIAL STATE - ZERO RPC for fresh Geyser launch"
            );
            let state = Self::initial_bonding_curve_state(creator);
            let amount_out = state.calculate_output(amount_in, buy_token);

            info!(
                token_mint=%token_mint_str,
                amount_in,
                amount_out,
                virtual_sol=state.virtual_sol_reserves,
                virtual_token=state.virtual_token_reserves,
                "pump.fun: calculated swap output (instant initial state - NO RPC)"
            );

            return Ok(Some(Quote {
                input_mint: input_mint.to_string(),
                output_mint: output_mint.to_string(),
                amount_out,
                price_impact_bps: 0,
                route: vec![bonding_curve.to_string()],
                fee_bps: 100, // 1% Pump.fun fee
                in_reserve: state.virtual_sol_reserves as u128,
                out_reserve: state.virtual_token_reserves as u128,
                tick_spacing: None,
            }));
        }

        // Only use RPC for non-CREATE scenarios (e.g., sells, or when no creator provided)
        const MAX_RETRIES: usize = 8;
        const INITIAL_DELAY_MS: u64 = 50;
        const MAX_DELAY_MS: u64 = 1000;

        let mut state_opt = None;

        for attempt in 0..MAX_RETRIES {
            if let Some(s) = self.fetch_bonding_curve_fast(&bonding_curve).await {
                if attempt > 0 {
                    info!(
                        token_mint=%token_mint_str,
                        bonding_curve=%bonding_curve,
                        attempt,
                        "pump.fun: bonding curve fetch succeeded after retry"
                    );
                } else {
                    debug!(
                        token_mint=%token_mint_str,
                        bonding_curve=%bonding_curve,
                        "pump.fun: bonding curve fetched on first try"
                    );
                }
                state_opt = Some(s);
                break;
            } else if attempt < MAX_RETRIES - 1 {
                // Exponential backoff: 50, 100, 200, 400, 800, 1000, 1000ms
                let delay = (INITIAL_DELAY_MS * (1 << attempt)).min(MAX_DELAY_MS);
                debug!(
                    token_mint=%token_mint_str,
                    bonding_curve=%bonding_curve,
                    attempt,
                    delay_ms=delay,
                    "pump.fun: bonding curve not found, retrying with backoff..."
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
            }
        }

        // If RPC fetch failed, use fallback with initial bonding curve state
        // VERIFIED: The creator from instruction_accounts[7] (User/Fee Payer)
        // IS the same as the creator stored in the bonding curve at bytes [49..81]
        let state = match state_opt {
            Some(s) => s,
            None => {
                // Use fallback with known initial state + creator from TX
                if use_fallback {
                    if let Some(creator) = fallback_creator {
                        info!(
                            token_mint=%token_mint_str,
                            bonding_curve=%bonding_curve,
                            creator=%creator,
                            "pump.fun: using FALLBACK initial state (RPC too slow, creator from TX instruction_accounts[7])"
                        );
                        Self::initial_bonding_curve_state(creator)
                    } else {
                        warn!(
                            token_mint=%token_mint_str,
                            bonding_curve=%bonding_curve,
                            "pump.fun: bonding curve not found, no fallback creator provided - SKIPPING"
                        );
                        return Ok(None);
                    }
                } else {
                    warn!(
                        token_mint=%token_mint_str,
                        bonding_curve=%bonding_curve,
                        "pump.fun: bonding curve not found after {} retries, fallback disabled - SKIPPING",
                        MAX_RETRIES
                    );
                    return Ok(None);
                }
            }
        };

        // Check if bonding curve is complete (migrated to PumpSwap AMM)
        if state.complete {
            info!(token_mint=%token_mint_str, bonding_curve=%bonding_curve, "pump.fun: bonding curve completed, migrated to pump_amm (PumpSwap)");
            return Ok(None);
        }

        // SELL: Validate against real_token_reserves (same logic as quote_exact_in).
        // If real_reserves=0 but virtual_reserves populated, cache may be stale — proceed.
        if !buy_token {
            let real_reserves_populated =
                state.real_token_reserves > 0 || state.real_sol_reserves > 0;
            if real_reserves_populated {
                if amount_in > state.real_token_reserves {
                    info!(
                        token_mint=%token_mint_str,
                        bonding_curve=%bonding_curve,
                        amount_in,
                        real_token_reserves=state.real_token_reserves,
                        "pump.fun: SELL skipped — amount exceeds real_token_reserves (with_fallback)"
                    );
                    return Ok(None);
                }
                if state.real_sol_reserves == 0 {
                    info!(
                        token_mint=%token_mint_str,
                        bonding_curve=%bonding_curve,
                        "pump.fun: SELL skipped — real_sol_reserves is 0 (with_fallback)"
                    );
                    return Ok(None);
                }
            } else if state.virtual_token_reserves > 0 {
                // Both real_token_reserves and real_sol_reserves are 0, but virtual reserves
                // are populated. For SELL, this almost certainly means the bonding curve has
                // been migrated to PumpSwap AMM. Reject and let multi-pool routing handle it.
                info!(
                    token_mint=%token_mint_str,
                    bonding_curve=%bonding_curve,
                    virtual_token=state.virtual_token_reserves,
                    virtual_sol=state.virtual_sol_reserves,
                    "pump.fun: SELL skipped — real_reserves both 0, likely migrated (with_fallback)"
                );
                return Ok(None);
            }
        }

        // Calculate output
        let amount_out = state.calculate_output(amount_in, buy_token);

        info!(
            token_mint=%token_mint_str,
            amount_in,
            amount_out,
            virtual_sol=state.virtual_sol_reserves,
            virtual_token=state.virtual_token_reserves,
            real_sol=state.real_sol_reserves,
            real_token=state.real_token_reserves,
            "pump.fun: calculated swap output (with_fallback)"
        );

        if amount_out == 0 {
            info!(token_mint=%token_mint_str, "pump.fun: amount_out is zero, no quote");
            return Ok(None);
        }

        // Calculate price impact
        let (in_reserve, out_reserve) = if buy_token {
            (
                state.virtual_sol_reserves as u128,
                state.virtual_token_reserves as u128,
            )
        } else {
            (
                state.virtual_token_reserves as u128,
                state.virtual_sol_reserves as u128,
            )
        };

        let price_impact_bps = if in_reserve > 0 {
            let impact = (amount_in as u128 * 10000) / in_reserve;
            impact.min(10000) as u32
        } else {
            0
        };

        Ok(Some(Quote {
            amount_out,
            price_impact_bps,
            route: vec![bonding_curve.to_string()],
            fee_bps: 100, // Pump.fun fee: 1%
            in_reserve,
            out_reserve,
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            tick_spacing: None,
        }))
    }
}

#[async_trait]
impl Dex for PumpFunDex {
    async fn refresh_pools(&self) -> Result<()> {
        // Pump.fun pools are discovered via Geyser, no refresh needed
        Ok(())
    }

    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>> {
        let sol_mint = "So11111111111111111111111111111111111111112";

        // Determine direction: SOL→Token or Token→SOL
        let (token_mint_str, buy_token) = if input_mint == sol_mint {
            (output_mint, true)
        } else if output_mint == sol_mint {
            (input_mint, false)
        } else {
            return Ok(None); // Pump.fun only does SOL pairs
        };

        let token_mint = Pubkey::from_str(token_mint_str)?;

        // CRITICAL: Verify token mint exists before wasting time on bonding curve
        // Geyser can report accounts from failed/rolled-back transactions
        /*
           DISABLED for sniping speed:
           RPC nodes are too slow to index the mint account immediately after creation.
           We trust the Geyser instruction parser. If the mint is invalid, the bonding curve fetch will fail anyway.

        match self.rpc.get_account_retry(&token_mint).await {
            Ok(_) => {
                debug!(
                    token_mint=%token_mint_str,
                    "pump.fun: token mint verified, proceeding to bonding curve"
                );
            }
            Err(e) => {
                info!(
                    token_mint=%token_mint_str,
                    error=%e,
                    "pump.fun: token mint does not exist (likely failed transaction from Geyser)"
                );
                return Ok(None);
            }
        }
        */

        let (bonding_curve, _bump) = self.derive_bonding_curve(&token_mint);

        // GEYSER-FIRST: Try LivePoolCache before any RPC call (PR2).
        // Cache is populated by market-data via Geyser account subscriptions.
        let state = if let Some(cached) = self.get_bonding_curve_from_cache(&bonding_curve) {
            debug!(
                token_mint=%token_mint_str,
                bonding_curve=%bonding_curve,
                buy_token,
                source="cache",
                "pump.fun: bonding curve from LivePoolCache (GEYSER-FIRST)"
            );
            cached
        } else {
            // RPC FALLBACK: Cache miss → fetch from RPC with retry.
            // This should only happen for brand-new pools that Geyser hasn't indexed yet.
            warn!(
                token_mint=%token_mint_str,
                bonding_curve=%bonding_curve,
                buy_token,
                "pump.fun: cache miss, falling back to RPC for bonding curve"
            );

            const MAX_RETRIES: usize = 5;
            const RETRY_DELAY_MS: u64 = 200;

            let mut state_opt = None;

            for attempt in 0..MAX_RETRIES {
                if let Some(s) = self.fetch_bonding_curve_fast(&bonding_curve).await {
                    if attempt > 0 {
                        info!(
                            token_mint=%token_mint_str,
                            bonding_curve=%bonding_curve,
                            attempt,
                            "pump.fun: bonding curve RPC fetch succeeded after retry"
                        );
                    }
                    state_opt = Some(s);
                    break;
                } else if attempt < MAX_RETRIES - 1 {
                    debug!(
                        token_mint=%token_mint_str,
                        bonding_curve=%bonding_curve,
                        attempt,
                        "pump.fun: bonding curve not found via RPC, retrying..."
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                }
            }

            match state_opt {
                Some(s) => s,
                None => {
                    info!(
                        token_mint=%token_mint_str,
                        bonding_curve=%bonding_curve,
                        "pump.fun: bonding curve not found after {} RPC retries - SKIPPING",
                        MAX_RETRIES
                    );
                    return Ok(None);
                }
            }
        };

        // Check if bonding curve is complete (migrated to PumpSwap AMM)
        if state.complete {
            info!(token_mint=%token_mint_str, bonding_curve=%bonding_curve, "pump.fun: bonding curve completed, migrated to pump_amm (PumpSwap)");
            return Ok(None);
        }

        // SELL: Validate against real_token_reserves BEFORE quoting.
        // PumpFun's on-chain sell function checks: `amount <= real_token_reserves`
        // (error 6023: NotEnoughTokensToSell). Without this check, the quote succeeds
        // but simulation fails, blocking multi-pool routing fallback.
        //
        // IMPORTANT: If real_reserves are 0 but virtual_reserves are populated,
        // the cache may be stale (PoolCacheUpdate from older market-data without real_reserves).
        // In that case, proceed with the quote and let simulation catch any on-chain failures.
        if !buy_token {
            let real_reserves_populated =
                state.real_token_reserves > 0 || state.real_sol_reserves > 0;
            if real_reserves_populated {
                if amount_in > state.real_token_reserves {
                    info!(
                        token_mint=%token_mint_str,
                        bonding_curve=%bonding_curve,
                        amount_in,
                        real_token_reserves=state.real_token_reserves,
                        "pump.fun: SELL skipped — amount exceeds real_token_reserves (6023 would fail on-chain)"
                    );
                    return Ok(None);
                }
                if state.real_sol_reserves == 0 {
                    info!(
                        token_mint=%token_mint_str,
                        bonding_curve=%bonding_curve,
                        real_sol_reserves=state.real_sol_reserves,
                        "pump.fun: SELL skipped — real_sol_reserves is 0 (curve has no SOL to pay out)"
                    );
                    return Ok(None);
                }
            } else if state.virtual_token_reserves > 0 {
                // Both real_token_reserves and real_sol_reserves are 0, but virtual reserves
                // are populated. For SELL, this almost certainly means the bonding curve has
                // been migrated to PumpSwap AMM. Generating a quote from stale virtual reserves
                // would succeed locally but fail on-chain with Custom(6023).
                // Reject the SELL and let multi-pool routing find the correct DEX (PumpSwap AMM).
                info!(
                    token_mint=%token_mint_str,
                    bonding_curve=%bonding_curve,
                    virtual_token=state.virtual_token_reserves,
                    virtual_sol=state.virtual_sol_reserves,
                    "pump.fun: SELL skipped — real_reserves both 0 (likely migrated to PumpSwap AMM)"
                );
                return Ok(None);
            }
        }

        // Calculate output
        let amount_out = state.calculate_output(amount_in, buy_token);

        info!(
            token_mint=%token_mint_str,
            amount_in,
            amount_out,
            virtual_sol=state.virtual_sol_reserves,
            virtual_token=state.virtual_token_reserves,
            real_sol=state.real_sol_reserves,
            real_token=state.real_token_reserves,
            "pump.fun: calculated swap output"
        );

        if amount_out == 0 {
            info!(token_mint=%token_mint_str, "pump.fun: amount_out is zero, no quote");
            return Ok(None);
        }

        // Calculate price impact
        let (in_reserve, out_reserve) = if buy_token {
            (
                state.virtual_sol_reserves as u128,
                state.virtual_token_reserves as u128,
            )
        } else {
            (
                state.virtual_token_reserves as u128,
                state.virtual_sol_reserves as u128,
            )
        };

        let price_impact_bps = if in_reserve > 0 {
            let impact = (amount_in as u128 * 10000) / in_reserve;
            impact.min(10000) as u32
        } else {
            0
        };

        Ok(Some(Quote {
            amount_out,
            price_impact_bps,
            route: vec![bonding_curve.to_string()],
            fee_bps: 100, // Pump.fun fee: 1%
            in_reserve,
            out_reserve,
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            tick_spacing: None,
        }))
    }

    fn build_swap_ix(
        &self,
        _input_mint: &str,
        _output_mint: &str,
        _amount_in: u64,
        _min_out: u64,
    ) -> Result<Vec<Instruction>> {
        // DEPRECATED: This sync function cannot get the creator from bonding curve
        // Use build_swap_ix_async instead which fetches creator from chain
        Err(anyhow!(
            "build_swap_ix is deprecated for Pump.fun - use build_swap_ix_async instead"
        ))
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        // Pump.fun pairs are dynamic, discovered via Geyser
        Vec::new()
    }

    /// Cache extra data for building swap instructions (GEYSER-FIRST).
    /// For PumpFun, we cache the creator for a token mint.
    /// Format: key = "creator:<token_mint>", value = "<creator_pubkey>"
    fn cache_extra_data(&self, key: &str, value: &str) {
        if let Some(mint_str) = key.strip_prefix("creator:") {
            if let (Ok(mint), Ok(creator)) = (Pubkey::from_str(mint_str), Pubkey::from_str(value)) {
                self.cache_creator(mint, creator);
                debug!(
                    token_mint = %mint,
                    creator = %creator,
                    "PumpFun: cached creator from Geyser data"
                );
            }
        }
    }

    /// Override the async build for PumpFun - routes to the real async implementation
    async fn build_swap_ix_async(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>> {
        // Use the impl-block async method (not trait method)
        PumpFunDex::build_swap_ix_async(self, input_mint, output_mint, amount_in, min_out, None)
            .await
    }
}

impl PumpFunDex {
    /// Async version of build_swap_ix that fetches bonding curve state to get creator
    /// This is the preferred method for building swap instructions
    ///
    /// For BUY (SOL → Token):
    ///   - amount_in: SOL lamports you want to spend
    ///   - min_out: minimum tokens you expect to receive (with slippage already applied)
    ///   - expected_out: expected tokens without slippage (needed to calculate max_sol_cost)
    ///   - slippage_bps: slippage tolerance in basis points (e.g., 1500 = 15%)
    ///
    /// For SELL (Token → SOL):
    ///   - amount_in: tokens to sell
    ///   - min_out: minimum SOL to receive
    pub async fn build_swap_ix_async(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
        fallback_creator: Option<Pubkey>, // Creator from Geyser event for fresh launches
    ) -> Result<Vec<Instruction>> {
        // Default slippage for backwards compatibility, no token_program override
        self.build_swap_ix_async_with_slippage(
            input_mint,
            output_mint,
            amount_in,
            min_out,
            fallback_creator,
            1500,
            None,  // No token_program override - uses default SPL Token
            false, // market_order: default limit order
            false, // allow_rpc_fallback: Hot Path default (GEYSER-ONLY)
        )
        .await
    }

    /// Build swap instruction with explicit slippage for proper max_sol_cost calculation
    ///
    /// # Arguments
    /// * `token_program_override` - If set, use this token program instead of defaulting to SPL Token.
    ///   This is critical for Token-2022 mints (newer PumpFun tokens).
    /// * `allow_rpc_fallback` - When true (Cold Path, e.g. Liquidation), use RPC for cashback_enabled
    ///   on cache miss. When false (Hot Path), cashback_enabled=false on cache miss (I-7).
    #[allow(clippy::too_many_arguments)]
    pub async fn build_swap_ix_async_with_slippage(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
        fallback_creator: Option<Pubkey>,
        slippage_bps: u32,
        token_program_override: Option<Pubkey>,
        market_order: bool,
        allow_rpc_fallback: bool,
    ) -> Result<Vec<Instruction>> {
        let sol_mint = "So11111111111111111111111111111111111111112";

        // Determine direction
        let (token_mint_str, buy_token) = if input_mint == sol_mint {
            (output_mint, true)
        } else if output_mint == sol_mint {
            (input_mint, false)
        } else {
            return Err(anyhow!("pump.fun only supports SOL pairs"));
        };

        let token_mint = Pubkey::from_str(token_mint_str)?;
        let token_program_sdk =
            self.token_program_for_mint(&token_mint, token_program_override.as_ref());
        let (bonding_curve, _bump) = self.derive_bonding_curve(&token_mint);
        let (associated_bonding_curve, _bump2) =
            self.derive_associated_bonding_curve(&bonding_curve, &token_mint, &token_program_sdk);

        // Guard: Reject builds for completed bonding curves (migrated to PumpSwap AMM).
        // Without this check, the tx builds successfully and simulation may pass, but
        // on-chain execution fails with Custom(2006) "A seeds constraint was violated"
        // because the bonding curve PDA is no longer valid after migration.
        if let Some(state) = self.get_bonding_curve_from_cache(&bonding_curve) {
            if state.complete {
                return Err(anyhow!(
                    "pump.fun bonding curve completed (migrated to pump_amm) for mint {} — \
                     cannot build swap instruction via bonding curve, use pump_amm instead",
                    token_mint_str
                ));
            }
        }

        // Creator resolution priority (GEYSER-FIRST, PR2):
        // 1. Explicit fallback_creator (from Geyser event/intent metadata)
        // 2. Cached creator (from DashMap, populated from previous lookups)
        // 3. LivePoolCache (GEYSER-FIRST, populated by market-data)
        // 4. RPC fallback (DEPRECATED - should not happen in production)
        // Cashback resolution: Cold Path MUST verify via RPC — Cache-HIT with stale
        // cashback_enabled=false must NOT short-circuit (Bug #25). If RPC fails → Err.
        // Hot Path: trust Geyser-fed cache only (I-7: no RPC).
        let cashback_from_rpc_or_cache = |allow_rpc: bool| async move {
            if allow_rpc {
                // Cold Path: ALWAYS verify via RPC. No fallback to cache on RPC failure.
                match self.fetch_bonding_curve(&bonding_curve).await {
                    Ok(state) => Ok(state.cashback_enabled),
                    Err(e) => Err(anyhow!(
                        "Cold Path: RPC verification of cashback_enabled failed for bonding curve {} — \
                         cannot proceed without authoritative on-chain state (RPC unreachable): {}",
                        bonding_curve,
                        e
                    )),
                }
            } else {
                Ok(self
                    .get_bonding_curve_from_cache(&bonding_curve)
                    .map(|s| s.cashback_enabled)
                    .unwrap_or(false))
            }
        };

        let (creator, cashback_enabled) = if let Some(c) = fallback_creator {
            debug!(
                token_mint = %token_mint_str,
                creator = %c,
                "pump.fun: using creator from producer"
            );
            let cashback = cashback_from_rpc_or_cache(allow_rpc_fallback).await?;
            (c, cashback)
        } else if let Some(c) = self.get_cached_creator(&token_mint) {
            debug!(
                token_mint = %token_mint_str,
                creator = %c,
                "pump.fun: using creator from DashMap cache"
            );
            let cashback = cashback_from_rpc_or_cache(allow_rpc_fallback).await?;
            (c, cashback)
        } else if let Some(c) = self.get_creator_from_cache(&bonding_curve) {
            debug!(
                token_mint = %token_mint_str,
                creator = %c,
                "pump.fun: using creator from LivePoolCache (GEYSER-FIRST)"
            );
            // Also cache in DashMap for faster subsequent lookups
            self.cached_creators.insert(token_mint, c);
            let cashback = cashback_from_rpc_or_cache(allow_rpc_fallback).await?;
            (c, cashback)
        } else {
            // RPC FALLBACK - Only allowed in Cold Path (Liquidation).
            // Hot Path: reject when creator not in cache (I-7).
            if !allow_rpc_fallback {
                return Err(anyhow!(
                    "pump.fun: creator not in cache (GEYSER-ONLY hot path, no RPC)"
                ));
            }
            warn!(
                token_mint = %token_mint_str,
                bonding_curve = %bonding_curve,
                "pump.fun: FALLBACK to RPC for creator - this indicates a cache miss!"
            );
            let acct = self.rpc.get_account_retry(&bonding_curve).await?;
            let state = BondingCurveState::parse(&acct.data)?;
            // Double-check: if RPC reveals complete, reject before wasting TX fees
            if state.complete {
                return Err(anyhow!(
                    "pump.fun bonding curve completed (from RPC fallback) for mint {} — \
                     cannot build swap instruction via bonding curve",
                    token_mint_str
                ));
            }
            // Cache for future use
            self.cached_creators.insert(token_mint, state.creator);
            (state.creator, state.cashback_enabled)
        };

        let user = self
            .user_authority
            .ok_or_else(|| anyhow!("user authority not set"))?;

        // Derive user token account (ATA) using the mint's actual token program.
        // If the mint is Token-2022, the ATA address and ATA-create instruction MUST use
        // the Token-2022 program id, otherwise simulation fails (IncorrectProgramId).
        let user_spl = sdk_pubkey_to_spl(&user);
        let token_mint_spl = sdk_pubkey_to_spl(&token_mint);
        let token_program_spl = sdk_pubkey_to_spl(&token_program_sdk);
        let user_token_account_spl =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &user_spl,
                &token_mint_spl,
                &token_program_spl,
            );
        let user_token_account = Pubkey::new_from_array(user_token_account_spl.to_bytes());

        let ix = if buy_token {
            if market_order {
                info!(
                    token_mint = %token_mint_str,
                    sol_amount = amount_in,
                    min_tokens_out = 1u64,
                    "pump.fun MARKET ORDER BUY: exact SOL in, min tokens out = 1"
                );
                self.build_buy_exact_sol_ix(
                    &token_mint,
                    &bonding_curve,
                    &associated_bonding_curve,
                    &user_token_account,
                    &creator,
                    &token_program_sdk,
                    amount_in,
                    1,
                )?
            } else {
                // For BUY: We want to spend amount_in SOL to get tokens.
                // Pump.fun BUY expects:
                //   - amount: token amount we want to receive (min_out)
                //   - max_sol_cost: maximum SOL we're willing to pay (with slippage buffer!)
                //
                // The slippage on BUY should be on max_sol_cost, not on token amount!
                // If price goes up, we need MORE SOL for the same tokens.
                // So max_sol_cost = amount_in + slippage
                let max_sol_cost =
                    ((amount_in as u128) * (10_000 + slippage_bps as u128) / 10_000) as u64;

                info!(
                    token_mint = %token_mint_str,
                    amount_in_sol = amount_in,
                    min_tokens_out = min_out,
                    max_sol_cost,
                    slippage_bps,
                    "pump.fun BUY: amount_in SOL with slippage protection on max_sol_cost"
                );

                self.build_buy_ix(
                    &token_mint,
                    &bonding_curve,
                    &associated_bonding_curve,
                    &user_token_account,
                    &creator,
                    &token_program_sdk,
                    min_out,      // amount (tokens we want)
                    max_sol_cost, // max SOL we're willing to pay (amount_in + slippage!)
                )?
            }
        } else {
            self.build_sell_ix(
                &token_mint,
                &bonding_curve,
                &associated_bonding_curve,
                &user_token_account,
                &creator,
                &token_program_sdk,
                amount_in,
                min_out,
                cashback_enabled,
            )?
        };

        // For BUY: Create ATA for receiving tokens (may not exist yet)
        // For SELL: Token ATA already exists (we own the tokens), no create needed
        //
        // The ATA create instruction uses GetAccountDataSize which requires the correct
        // token program. For Token-2022, using spl_token::id() causes IncorrectProgramId.
        // By skipping ATA create for SELL, we avoid this issue entirely.
        if buy_token {
            // BUY: Prepend idempotent ATA creation - user may not have token ATA yet
            let create_ata_ix_prog =
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &user_spl, // payer
                    &user_spl, // owner
                    &token_mint_spl,
                    &token_program_spl,
                );
            let create_ata_ix = prog_ix_to_sdk(create_ata_ix_prog);
            Ok(vec![create_ata_ix, ix])
        } else {
            // SELL: Token ATA must exist (we're selling tokens we own)
            // SOL is received directly via system transfer, no WSOL ATA needed for BC
            Ok(vec![ix])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_fee_config_matches_known_address() {
        // Known fee_config address from successful Solscan TX (Dec 2024)
        // https://solscan.io/tx/2YH9QaRJy6LA6HExgFDCNtx2ccqESXwprJhtr8Vah9snfDkXz9mhWeiKZGBevLCRs985VMpaAY8JgZ37EhiVi9jU
        let expected = Pubkey::from_str("8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt").unwrap();
        let (derived, _bump) = PumpFunDex::derive_fee_config();
        assert_eq!(
            derived, expected,
            "Fee config PDA mismatch! Derived: {}, Expected: {}",
            derived, expected
        );
    }

    #[test]
    fn test_derive_global_volume_accumulator() {
        // Known global_volume_accumulator from successful TX
        let expected = Pubkey::from_str("Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y").unwrap();

        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();
        let (derived, _bump) =
            Pubkey::find_program_address(&[b"global_volume_accumulator"], &program_id);
        assert_eq!(
            derived, expected,
            "Global volume accumulator PDA mismatch! Derived: {}, Expected: {}",
            derived, expected
        );
    }

    #[test]
    fn test_derive_creator_vault_structure() {
        // Test that derive_creator_vault produces a valid PDA
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();
        let test_creator =
            Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM").unwrap();

        let (vault, bump) =
            Pubkey::find_program_address(&[b"creator-vault", test_creator.as_ref()], &program_id);

        // bump is u8 (0-255) by definition from find_program_address
        // Just verify we got a valid PDA by checking vault is not default
        let _ = bump; // Acknowledge bump exists
        assert_ne!(
            vault,
            Pubkey::default(),
            "Creator vault should not be default pubkey"
        );

        // PDAs are OFF-curve by design (no private key exists)
        // is_on_curve() returns false for valid PDAs
        assert!(
            !vault.is_on_curve(),
            "Creator vault PDA should be off-curve (no private key)"
        );
    }

    #[test]
    fn test_build_buy_ix_data_includes_track_volume_false() {
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:8899"));
        let mut dex = PumpFunDex::new(rpc, None).expect("PumpFunDex::new");
        let wallet =
            Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM").expect("wallet");
        dex.set_user_authority(wallet);

        let creator =
            Pubkey::from_str("2tFqgkJX6kqz8q6o9tFv3oJ9nQx7n1m3fHk2m8f3oKpZ").expect("creator");
        let token_mint =
            Pubkey::from_str("9xQeWvG816bUx9EPfKJb9N9dKz5wW7Yy2hBzXv4mQ4kG").expect("mint");
        let (bonding_curve, _) = dex.derive_bonding_curve(&token_mint);
        let token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        let (associated_bonding_curve, _) =
            dex.derive_associated_bonding_curve(&bonding_curve, &token_mint, &token_program);
        let user_token_account =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &sdk_pubkey_to_spl(&wallet),
                &sdk_pubkey_to_spl(&token_mint),
                &sdk_pubkey_to_spl(&token_program),
            );
        let user_token_account = Pubkey::new_from_array(user_token_account.to_bytes());

        let ix = dex
            .build_buy_ix(
                &token_mint,
                &bonding_curve,
                &associated_bonding_curve,
                &user_token_account,
                &creator,
                &token_program,
                1_000_000,
                1_050_000,
            )
            .expect("build_buy_ix");

        assert_eq!(ix.data.len(), 25);
        assert_eq!(
            ix.data[24], 0,
            "track_volume=false → OptionBool encodes as 0"
        );

        assert_eq!(
            ix.accounts.len(),
            18,
            "BUY must have 18 accounts (buyback fee recipient after v2)"
        );
        let expected_buyback =
            Pubkey::from_str(PUMPFUN_BUYBACK_FEE_RECIPIENT).expect("buyback pubkey");
        let (bonding_curve_v2, _) = PumpFunDex::derive_bonding_curve_v2(&token_mint);
        assert_eq!(ix.accounts[16].pubkey, bonding_curve_v2);
        assert!(!ix.accounts[16].is_writable, "bonding_curve_v2 is readonly");
        let tail = ix.accounts.last().expect("buyback tail");
        assert_eq!(tail.pubkey, expected_buyback);
        assert!(tail.is_writable, "buyback fee recipient must be writable");
        assert!(!tail.is_signer);
    }

    #[test]
    fn test_build_buy_exact_sol_ix_data_includes_track_volume_false() {
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:8899"));
        let mut dex = PumpFunDex::new(rpc, None).expect("PumpFunDex::new");
        let wallet =
            Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM").expect("wallet");
        dex.set_user_authority(wallet);

        let creator =
            Pubkey::from_str("2tFqgkJX6kqz8q6o9tFv3oJ9nQx7n1m3fHk2m8f3oKpZ").expect("creator");
        let token_mint =
            Pubkey::from_str("9xQeWvG816bUx9EPfKJb9N9dKz5wW7Yy2hBzXv4mQ4kG").expect("mint");
        let (bonding_curve, _) = dex.derive_bonding_curve(&token_mint);
        let token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        let (associated_bonding_curve, _) =
            dex.derive_associated_bonding_curve(&bonding_curve, &token_mint, &token_program);
        let user_token_account =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &sdk_pubkey_to_spl(&wallet),
                &sdk_pubkey_to_spl(&token_mint),
                &sdk_pubkey_to_spl(&token_program),
            );
        let user_token_account = Pubkey::new_from_array(user_token_account.to_bytes());

        let ix = dex
            .build_buy_exact_sol_ix(
                &token_mint,
                &bonding_curve,
                &associated_bonding_curve,
                &user_token_account,
                &creator,
                &token_program,
                1_250_000,
                1,
            )
            .expect("build_buy_exact_sol_ix");

        assert_eq!(ix.data.len(), 25);
        assert_eq!(
            ix.data[0..8],
            [56, 252, 116, 8, 158, 223, 205, 95],
            "buy_exact_sol_in discriminator"
        );
        assert_eq!(
            ix.data[24], 0,
            "track_volume=false → OptionBool encodes as 0"
        );

        assert_eq!(
            ix.accounts.len(),
            18,
            "buy_exact_sol_in must have 18 accounts (buyback fee recipient after v2)"
        );
        let expected_buyback =
            Pubkey::from_str(PUMPFUN_BUYBACK_FEE_RECIPIENT).expect("buyback pubkey");
        let (bonding_curve_v2, _) = PumpFunDex::derive_bonding_curve_v2(&token_mint);
        assert_eq!(ix.accounts[16].pubkey, bonding_curve_v2);
        assert!(!ix.accounts[16].is_writable);
        let tail = ix.accounts.last().expect("buyback tail");
        assert_eq!(tail.pubkey, expected_buyback);
        assert!(tail.is_writable);
        assert!(!tail.is_signer);
    }

    #[test]
    fn test_build_sell_ix_non_cashback_tail_buyback_fee_recipient() {
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:8899"));
        let mut dex = PumpFunDex::new(rpc, None).expect("PumpFunDex::new");
        let wallet =
            Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM").expect("wallet");
        dex.set_user_authority(wallet);

        let creator =
            Pubkey::from_str("2tFqgkJX6kqz8q6o9tFv3oJ9nQx7n1m3fHk2m8f3oKpZ").expect("creator");
        let token_mint =
            Pubkey::from_str("9xQeWvG816bUx9EPfKJb9N9dKz5wW7Yy2hBzXv4mQ4kG").expect("mint");
        let (bonding_curve, _) = dex.derive_bonding_curve(&token_mint);
        let token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        let (associated_bonding_curve, _) =
            dex.derive_associated_bonding_curve(&bonding_curve, &token_mint, &token_program);
        let user_token_account =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &sdk_pubkey_to_spl(&wallet),
                &sdk_pubkey_to_spl(&token_mint),
                &sdk_pubkey_to_spl(&token_program),
            );
        let user_token_account = Pubkey::new_from_array(user_token_account.to_bytes());

        let ix = dex
            .build_sell_ix(
                &token_mint,
                &bonding_curve,
                &associated_bonding_curve,
                &user_token_account,
                &creator,
                &token_program,
                1_000_000,
                1,
                false,
            )
            .expect("build_sell_ix non-cashback");

        assert_eq!(ix.accounts.len(), 16);
        let expected_buyback =
            Pubkey::from_str(PUMPFUN_BUYBACK_FEE_RECIPIENT).expect("buyback pubkey");
        let (bonding_curve_v2, _) = PumpFunDex::derive_bonding_curve_v2(&token_mint);
        assert_eq!(ix.accounts[14].pubkey, bonding_curve_v2);
        assert!(!ix.accounts[14].is_writable);
        let tail = ix.accounts.last().expect("buyback tail");
        assert_eq!(tail.pubkey, expected_buyback);
        assert!(tail.is_writable);
        assert!(!tail.is_signer);
    }

    #[test]
    fn test_build_sell_ix_cashback_tail_buyback_fee_recipient() {
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:8899"));
        let mut dex = PumpFunDex::new(rpc, None).expect("PumpFunDex::new");
        let wallet =
            Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM").expect("wallet");
        dex.set_user_authority(wallet);

        let creator =
            Pubkey::from_str("2tFqgkJX6kqz8q6o9tFv3oJ9nQx7n1m3fHk2m8f3oKpZ").expect("creator");
        let token_mint =
            Pubkey::from_str("9xQeWvG816bUx9EPfKJb9N9dKz5wW7Yy2hBzXv4mQ4kG").expect("mint");
        let (bonding_curve, _) = dex.derive_bonding_curve(&token_mint);
        let token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        let (associated_bonding_curve, _) =
            dex.derive_associated_bonding_curve(&bonding_curve, &token_mint, &token_program);
        let user_token_account =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &sdk_pubkey_to_spl(&wallet),
                &sdk_pubkey_to_spl(&token_mint),
                &sdk_pubkey_to_spl(&token_program),
            );
        let user_token_account = Pubkey::new_from_array(user_token_account.to_bytes());

        let ix = dex
            .build_sell_ix(
                &token_mint,
                &bonding_curve,
                &associated_bonding_curve,
                &user_token_account,
                &creator,
                &token_program,
                1_000_000,
                1,
                true,
            )
            .expect("build_sell_ix cashback");

        assert_eq!(ix.accounts.len(), 17);
        let expected_buyback =
            Pubkey::from_str(PUMPFUN_BUYBACK_FEE_RECIPIENT).expect("buyback pubkey");
        let (bonding_curve_v2, _) = PumpFunDex::derive_bonding_curve_v2(&token_mint);
        assert_eq!(ix.accounts[15].pubkey, bonding_curve_v2);
        assert!(!ix.accounts[15].is_writable);
        let tail = ix.accounts.last().expect("buyback tail");
        assert_eq!(tail.pubkey, expected_buyback);
        assert!(tail.is_writable);
        assert!(!tail.is_signer);
    }
}
