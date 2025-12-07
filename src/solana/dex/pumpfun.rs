//! Pump.fun DEX Integration
//! Bonding curve-based token swaps
//! Program: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

use crate::solana::rpc::SolanaRpc;
use super::{Dex, Quote};

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

/// Bonding curve account data layout
#[derive(Debug, Clone)]
pub struct BondingCurveState {
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub token_mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub complete: bool,
}

impl BondingCurveState {
    /// Parse bonding curve account data
    /// Layout (from reverse engineering):
    /// - 0-8: discriminator/version
    /// - 8-16: virtual_token_reserves (u64)
    /// - 16-24: virtual_sol_reserves (u64)
    /// - 24-32: real_token_reserves (u64)
    /// - 32-40: real_sol_reserves (u64)
    /// - 40-72: token_mint (Pubkey)
    /// - 72-104: bonding_curve (Pubkey)
    /// - 104: complete (bool)
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 105 {
            return Err(anyhow!("bonding curve data too short: {} bytes", data.len()));
        }

        Ok(Self {
            virtual_token_reserves: u64::from_le_bytes(data[8..16].try_into()?),
            virtual_sol_reserves: u64::from_le_bytes(data[16..24].try_into()?),
            real_token_reserves: u64::from_le_bytes(data[24..32].try_into()?),
            real_sol_reserves: u64::from_le_bytes(data[32..40].try_into()?),
            token_mint: Pubkey::new_from_array(data[40..72].try_into()?),
            bonding_curve: Pubkey::new_from_array(data[72..104].try_into()?),
            complete: data[104] != 0,
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
}

impl PumpFunDex {
    pub fn new(rpc: Arc<SolanaRpc>) -> Result<Self> {
        Ok(Self {
            rpc,
            program_id: Pubkey::from_str(PUMPFUN_PROGRAM_ID)?,
            global: Pubkey::from_str(PUMPFUN_GLOBAL)?,
            fee_recipient: Pubkey::from_str(PUMPFUN_FEE_RECIPIENT)?,
            event_authority: Pubkey::from_str(PUMPFUN_EVENT_AUTHORITY)?,
            user_authority: None,
        })
    }

    pub fn set_user_authority(&mut self, authority: Pubkey) {
        self.user_authority = Some(authority);
    }

    /// Derive bonding curve PDA for a token mint
    pub fn derive_bonding_curve(&self, token_mint: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[b"bonding-curve", token_mint.as_ref()],
            &self.program_id,
        )
    }

    /// Derive associated bonding curve token account (holds real token reserves)
    pub fn derive_associated_bonding_curve(&self, bonding_curve: &Pubkey, token_mint: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[
                b"associated-bonding-curve",
                bonding_curve.as_ref(),
                token_mint.as_ref(),
            ],
            &self.program_id,
        )
    }

    /// Fetch bonding curve state from chain
    pub async fn fetch_bonding_curve(&self, bonding_curve: &Pubkey) -> Result<BondingCurveState> {
        let account = self.rpc.get_account_retry(bonding_curve).await?;
        BondingCurveState::parse(&account.data)
    }

    /// Build buy instruction (SOL → Token)
    /// Instruction discriminator: 0x66063d1201daebea (8 bytes)
    pub fn build_buy_ix(
        &self,
        token_mint: &Pubkey,
        bonding_curve: &Pubkey,
        associated_bonding_curve: &Pubkey,
        user_token_account: &Pubkey,
        amount_in: u64,  // SOL lamports
        max_sol_cost: u64, // Slippage protection
    ) -> Result<Instruction> {
        let user = self.user_authority
            .ok_or_else(|| anyhow!("user authority not set"))?;

        // Instruction data: discriminator (8 bytes) + amount (8 bytes) + max_cost (8 bytes)
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&0x66063d1201daebea_u64.to_le_bytes()); // Buy discriminator
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&max_sol_cost.to_le_bytes());

        Ok(Instruction {
            program_id: self.program_id,
            accounts: vec![
                AccountMeta::new_readonly(self.global, false),
                AccountMeta::new(self.fee_recipient, false),
                AccountMeta::new_readonly(*token_mint, false),
                AccountMeta::new(*bonding_curve, false),
                AccountMeta::new(*associated_bonding_curve, false),
                AccountMeta::new(*user_token_account, false),
                AccountMeta::new(user, true), // Signer
                AccountMeta::new_readonly(Pubkey::from_str(SYSTEM_PROGRAM_ID).unwrap(), false),
                AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false),
                AccountMeta::new_readonly(sysvar::rent::id(), false),
                AccountMeta::new_readonly(self.event_authority, false),
                AccountMeta::new_readonly(self.program_id, false),
            ],
            data,
        })
    }

    /// Build sell instruction (Token → SOL)
    /// Instruction discriminator: 0x33e685a4017f83ad (8 bytes)
    pub fn build_sell_ix(
        &self,
        token_mint: &Pubkey,
        bonding_curve: &Pubkey,
        associated_bonding_curve: &Pubkey,
        user_token_account: &Pubkey,
        amount_in: u64,  // Token amount
        min_sol_output: u64, // Slippage protection
    ) -> Result<Instruction> {
        let user = self.user_authority
            .ok_or_else(|| anyhow!("user authority not set"))?;

        // Instruction data: discriminator (8 bytes) + amount (8 bytes) + min_output (8 bytes)
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&0x33e685a4017f83ad_u64.to_le_bytes()); // Sell discriminator
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_sol_output.to_le_bytes());

        Ok(Instruction {
            program_id: self.program_id,
            accounts: vec![
                AccountMeta::new_readonly(self.global, false),
                AccountMeta::new(self.fee_recipient, false),
                AccountMeta::new_readonly(*token_mint, false),
                AccountMeta::new(*bonding_curve, false),
                AccountMeta::new(*associated_bonding_curve, false),
                AccountMeta::new(*user_token_account, false),
                AccountMeta::new(user, true), // Signer
                AccountMeta::new_readonly(Pubkey::from_str(SYSTEM_PROGRAM_ID).unwrap(), false),
                AccountMeta::new_readonly(Pubkey::new_from_array(spl_associated_token_account::id().to_bytes()), false),
                AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false),
                AccountMeta::new_readonly(self.event_authority, false),
                AccountMeta::new_readonly(self.program_id, false),
            ],
            data,
        })
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
        let (bonding_curve, _bump) = self.derive_bonding_curve(&token_mint);

        info!(
            token_mint=%token_mint_str, 
            bonding_curve=%bonding_curve,
            buy_token,
            "pump.fun: attempting to fetch bonding curve"
        );

        // Fetch bonding curve state with retry logic for brand new tokens
        // New tokens may not have bonding curve account created yet
        let state = {
            const MAX_RETRIES: usize = 5;
            const RETRY_DELAY_MS: u64 = 30;
            
            let mut last_error = None;
            let mut state_opt = None;
            
            for attempt in 0..MAX_RETRIES {
                match self.fetch_bonding_curve(&bonding_curve).await {
                    Ok(s) => {
                        if attempt > 0 {
                            info!(
                                token_mint=%token_mint_str, 
                                bonding_curve=%bonding_curve, 
                                attempt, 
                                "pump.fun: bonding curve fetch succeeded after retry"
                            );
                        }
                        state_opt = Some(s);
                        break;
                    }
                    Err(e) => {
                        last_error = Some(e);
                        if attempt < MAX_RETRIES - 1 {
                            debug!(
                                token_mint=%token_mint_str, 
                                bonding_curve=%bonding_curve, 
                                attempt, 
                                "pump.fun: bonding curve not found, retrying..."
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                        }
                    }
                }
            }
            
            match state_opt {
                Some(s) => s,
                None => {
                    info!(
                        token_mint=%token_mint_str, 
                        bonding_curve=%bonding_curve, 
                        error=?last_error, 
                        "pump.fun: failed to fetch bonding curve after {} retries (account may not exist)", 
                        MAX_RETRIES
                    );
                    return Ok(None);
                }
            }
        };

        // Check if bonding curve is complete (migrated to Raydium)
        if state.complete {
            info!(token_mint=%token_mint_str, bonding_curve=%bonding_curve, "pump.fun: bonding curve completed, migrated to raydium");
            return Ok(None);
        }

        // Calculate output
        let amount_out = state.calculate_output(amount_in, buy_token);

        info!(
            token_mint=%token_mint_str,
            amount_in,
            amount_out,
            virtual_sol=state.virtual_sol_reserves,
            virtual_token=state.virtual_token_reserves,
            "pump.fun: calculated swap output"
        );

        if amount_out == 0 {
            info!(token_mint=%token_mint_str, "pump.fun: amount_out is zero, no quote");
            return Ok(None);
        }

        // Calculate price impact
        let (in_reserve, out_reserve) = if buy_token {
            (state.virtual_sol_reserves as u128, state.virtual_token_reserves as u128)
        } else {
            (state.virtual_token_reserves as u128, state.virtual_sol_reserves as u128)
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
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
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
        let (bonding_curve, _bump) = self.derive_bonding_curve(&token_mint);
        let (associated_bonding_curve, _bump2) = 
            self.derive_associated_bonding_curve(&bonding_curve, &token_mint);

        let user = self.user_authority
            .ok_or_else(|| anyhow!("user authority not set"))?;

        // Derive user token account (ATA)
        // Convert to spl_token Pubkey for ATA derivation
        let user_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(user.to_bytes());
        let token_mint_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(token_mint.to_bytes());
        let user_token_account_spl = spl_associated_token_account::get_associated_token_address(
            &user_spl,
            &token_mint_spl,
        );
        let user_token_account = Pubkey::new_from_array(user_token_account_spl.to_bytes());

        let ix = if buy_token {
            // Buy: SOL → Token
            self.build_buy_ix(
                &token_mint,
                &bonding_curve,
                &associated_bonding_curve,
                &user_token_account,
                amount_in,
                amount_in, // max_sol_cost = amount_in (no slippage limit here, use min_out instead)
            )?
        } else {
            // Sell: Token → SOL
            self.build_sell_ix(
                &token_mint,
                &bonding_curve,
                &associated_bonding_curve,
                &user_token_account,
                amount_in,
                min_out,
            )?
        };

        Ok(vec![ix])
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        // Pump.fun pairs are dynamic, discovered via Geyser
        Vec::new()
    }
}

// Missing sysvar module, add this
mod sysvar {
    pub mod rent {
        use solana_sdk::pubkey::Pubkey;
        use std::str::FromStr;
        
        pub fn id() -> Pubkey {
            Pubkey::from_str("SysvarRent111111111111111111111111111111111")
                .expect("valid rent sysvar")
        }
    }
}
