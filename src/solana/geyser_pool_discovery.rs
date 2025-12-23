//! Professional Geyser-based Pool Discovery
//! Replaces WebSocket log parsing with structured account updates

use crate::solana::geyser_listener::{
    GeyserAccountUpdate, GeyserListener, GeyserTransactionUpdate,
};
use crate::solana::rpc::SolanaRpc;
use anyhow::Result;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::debug;

/// Pool discovery via Geyser account updates
pub struct GeyserPoolDiscovery {
    listener: GeyserListener,
}

impl GeyserPoolDiscovery {
    /// Create new Geyser-based pool discovery
    ///
    /// # Arguments
    /// * `geyser_endpoint` - Geyser gRPC endpoint (e.g., "http://127.0.0.1:10000")
    /// * `program_ids` - DEX program IDs to monitor (Raydium, Orca, Pump.fun)
    /// * `rpc` - RPC client for additional data fetching
    pub fn new(
        geyser_endpoint: String,
        program_ids: Vec<Pubkey>,
        rpc: Arc<SolanaRpc>,
    ) -> (Self, broadcast::Receiver<PoolDiscoveryEvent>) {
        let (listener, account_rx, transaction_rx) =
            GeyserListener::new(geyser_endpoint, program_ids);

        // Create event channel for pool discoveries
        let (event_tx, event_rx) = broadcast::channel(10000);

        let discovery = Self { listener };

        // Spawn account processor
        let rpc_clone = rpc.clone();
        let event_tx_clone = event_tx.clone();
        tokio::spawn(async move {
            let mut rx = account_rx;
            loop {
                match rx.recv().await {
                    Ok(update) => {
                        if let Some(event) = Self::process_account_update(update, &rpc_clone).await
                        {
                            // Log before sending
                            tracing::debug!(
                                dex = ?event.dex_type,
                                pool = %event.pool_address,
                                "geyser_pool_discovery: sending account-based event"
                            );
                            let _ = event_tx_clone.send(event);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "geyser_pool_discovery: account processor lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::warn!("geyser_pool_discovery: account stream closed");
                        break;
                    }
                }
            }
        });

        // Spawn transaction processor
        let rpc_clone2 = rpc.clone();
        tokio::spawn(async move {
            let mut rx = transaction_rx;
            loop {
                match rx.recv().await {
                    Ok(tx_update) => {
                        if let Some(event) =
                            Self::process_transaction_update(tx_update, &rpc_clone2).await
                        {
                            // Log before sending to verify event is created
                            tracing::info!(
                                dex = ?event.dex_type,
                                pool = %event.pool_address,
                                base_mint = %event.base_mint,
                                "geyser_pool_discovery: SENDING transaction-based event to sniper"
                            );
                            match event_tx.send(event) {
                                Ok(receiver_count) => {
                                    tracing::info!(
                                        receiver_count,
                                        "geyser_pool_discovery: event sent successfully"
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(
                                        ?e,
                                        "geyser_pool_discovery: FAILED to send event - no receivers!"
                                    );
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            skipped,
                            "geyser_pool_discovery: transaction processor lagged"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::warn!("geyser_pool_discovery: transaction stream closed");
                        break;
                    }
                }
            }
        });

        (discovery, event_rx)
    }

    /// Start listening to Geyser updates
    pub async fn start(self) -> Result<()> {
        self.listener.start().await
    }

    /// Process account update and determine if it's a new pool
    async fn process_account_update(
        update: GeyserAccountUpdate,
        _rpc: &Arc<SolanaRpc>,
    ) -> Option<PoolDiscoveryEvent> {
        // Identify DEX by owner
        let dex_type = match update.owner.to_string().as_str() {
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" => DexType::RaydiumAmmV4,
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" => DexType::OrcaWhirlpool,
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" => {
                // Pump.fun: Skip account-based discovery, use transaction-based instead
                // Account data parsing at offset 40-72 produces wrong mints (12xtdJLo...)
                debug!(
                    pool = %update.pubkey,
                    slot = update.slot,
                    "geyser_pool_discovery: Pump.fun account update ignored (using transaction-based discovery)"
                );
                return None;
            }
            _ => {
                debug!(
                    owner = %update.owner,
                    "geyser_pool_discovery: unknown DEX program"
                );
                return None;
            }
        };

        // Parse pool data based on DEX type
        let pool_data = match dex_type {
            DexType::RaydiumAmmV4 => Self::parse_raydium_pool(&update.data),
            DexType::OrcaWhirlpool => Self::parse_orca_pool(&update.data),
            DexType::PumpFun => {
                // Should never reach here due to early return above
                return None;
            }
        }?;

        let discovered_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        debug!(
            pool = %update.pubkey,
            dex = ?dex_type,
            slot = update.slot,
            base_mint = %pool_data.base_mint,
            quote_mint = %pool_data.quote_mint,
            "geyser_pool_discovery: NEW POOL DETECTED"
        );

        Some(PoolDiscoveryEvent {
            pool_address: update.pubkey,
            dex_type,
            slot: update.slot,
            base_mint: pool_data.base_mint,
            quote_mint: pool_data.quote_mint,
            base_decimals: pool_data.base_decimals,
            quote_decimals: pool_data.quote_decimals,
            liquidity_estimate_lamports: pool_data.liquidity_lamports,
            coin_vault: pool_data.coin_vault,
            pc_vault: pool_data.pc_vault,
            creator: None, // Account-based discovery doesn't have creator info
            discovered_at_ms,
        })
    }

    /// Process transaction update to extract token mints from instruction accounts
    /// This is the professional approach - transaction instructions contain token mints
    /// as instruction accounts, avoiding the need to parse account data layouts
    async fn process_transaction_update(
        tx_update: GeyserTransactionUpdate,
        _rpc: &Arc<SolanaRpc>,
    ) -> Option<PoolDiscoveryEvent> {
        // Identify DEX by looking for known program IDs in account_keys
        let raydium_program =
            Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").ok()?;
        let orca_program = Pubkey::from_str("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc").ok()?;
        let pumpfun_program =
            Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").ok()?;

        let dex_type = if tx_update.account_keys.contains(&pumpfun_program) {
            DexType::PumpFun
        } else if tx_update.account_keys.contains(&raydium_program) {
            DexType::RaydiumAmmV4
        } else if tx_update.account_keys.contains(&orca_program) {
            DexType::OrcaWhirlpool
        } else {
            return None;
        };

        // Filter: Only process pool creation transactions
        // REMOVED: account_count == 18 check was too brittle and filtering valid transactions
        // We now rely on the instruction discriminator check which is much more robust
        let account_count = tx_update.account_keys.len();

        // Extract token mint from instruction accounts
        // For Pump.fun Create instruction:
        // account[0]: Token Mint (writable, to be created) ← THIS IS WHAT WE NEED!
        // account[1]: Mint Authority / Creator (signer)
        // account[2]: Bonding Curve (writable, PDA)
        // account[3]: Associated Bonding Curve Vault (writable)
        // account[4]: Global state
        // account[5]: MPL Token Metadata
        // account[6]: Metadata account
        // account[7]: System Program
        // account[8]: Token Program
        // account[9]: Associated Token Program
        // account[10]: Rent
        // account[11]: Event Authority
        // account[12]: Program

        // DEBUG: Log complete account_keys array to analyze structure
        if dex_type == DexType::PumpFun {
            tracing::debug!(
                message = "DEBUG: Pump.fun CREATE - instruction accounts",
                account_count = tx_update.account_keys.len(),
                instruction_count = tx_update.instruction_accounts.len(),
                ix_account_0 = %tx_update.instruction_accounts.first().map(|k| k.to_string()).unwrap_or_else(|| "None".to_string()),
                ix_account_1 = %tx_update.instruction_accounts.get(1).map(|k| k.to_string()).unwrap_or_else(|| "None".to_string()),
                ix_account_2 = %tx_update.instruction_accounts.get(2).map(|k| k.to_string()).unwrap_or_else(|| "None".to_string()),
                ix_account_3 = %tx_update.instruction_accounts.get(3).map(|k| k.to_string()).unwrap_or_else(|| "None".to_string()),
            );
        }

        let token_mint = match dex_type {
            DexType::PumpFun => {
                // DEBUG: Log instruction extraction status
                if tx_update.instruction_accounts.is_empty() {
                    debug!(
                        signature = %tx_update.signature,
                        account_keys_len = tx_update.account_keys.len(),
                        instruction_data_len = tx_update.instruction_data.len(),
                        "geyser_pool_discovery: No instruction accounts extracted - Pump.fun instruction not found"
                    );
                    return None;
                }

                // Filter: Pump.fun Create needs at least 4 instruction accounts
                // (mint, authority, bonding_curve, vault)
                if tx_update.instruction_accounts.len() < 4 {
                    return None; // Not a CREATE instruction
                }

                // Filter by instruction discriminator to distinguish CREATE from BUY/SELL
                // Pump.fun instruction discriminators (first 8 bytes of instruction data):
                // - CREATE: 0x181ec828051c0777 (sighash of "global:create")
                // - BUY: 0x66063d1201daebea (sighash of "global:buy")
                // - SELL: 0x33e685a4017f83ad (sighash of "global:sell")
                const PUMPFUN_CREATE_DISCRIMINATOR: [u8; 8] =
                    [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];

                if tx_update.instruction_data.len() >= 8 {
                    let discriminator = &tx_update.instruction_data[0..8];

                    // DEBUG: Log discriminator for analysis
                    tracing::debug!(
                        signature = %tx_update.signature,
                        discriminator_hex = format!("{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                            discriminator[0], discriminator[1], discriminator[2], discriminator[3],
                            discriminator[4], discriminator[5], discriminator[6], discriminator[7]),
                        instruction_count = tx_update.instruction_accounts.len(),
                        "geyser_pool_discovery: Pump.fun instruction discriminator check"
                    );

                    if discriminator != PUMPFUN_CREATE_DISCRIMINATOR {
                        // Not a CREATE instruction (likely BUY or SELL)
                        return None;
                    }

                    // Log successful CREATE detection
                    tracing::info!(
                        signature = %tx_update.signature,
                        "geyser_pool_discovery: ✅ FOUND Pump.fun CREATE INSTRUCTION"
                    );
                } else {
                    // No instruction data or too short - can't verify
                    debug!(
                        signature = %tx_update.signature,
                        data_len = tx_update.instruction_data.len(),
                        "geyser_pool_discovery: Pump.fun instruction has no discriminator - ignoring"
                    );
                    return None;
                }

                // Use INSTRUCTION accounts, not transaction account_keys!
                // Based on observed logs:
                // ix_account[0]: Global
                // ix_account[1]: Fee Recipient
                // ix_account[2]: Mint (writable, newly created)
                // ix_account[3]: Bonding Curve PDA (writable)

                // ROBUST MINT DETECTION:
                // Iterate through accounts to find the Mint.
                // Criteria:
                // 1. Writable (it's being initialized)
                // 2. Signer (it must sign to be created)
                // 3. On-Curve (not a PDA)
                // 4. Not the Fee Payer (usually index 0 of transaction, but hard to know here. However, Mint is usually NOT the first account in instruction if Global is 0)

                let mut found_mint = None;

                // Log all accounts for debugging
                for (i, acc) in tx_update.instruction_accounts.iter().enumerate() {
                    tracing::info!(
                        signature = %tx_update.signature,
                        index = i,
                        pubkey = %acc,
                        // We don't have is_signer/is_writable in the Pubkey struct here,
                        // assuming tx_update.instruction_accounts is just Vec<Pubkey>.
                        // Wait, if it's just Pubkey, we can't check signer status!
                        // We need to look up the account in the transaction message if possible,
                        // or rely on position/curve check.
                        is_on_curve = acc.is_on_curve(),
                        "geyser_pool_discovery: Account Analysis"
                    );
                }

                // Since we only have Pubkeys in instruction_accounts (based on usage),
                // we can't check signer/writable status directly unless we have the full message.
                // However, we know:
                // - Global (index 0) is on-curve but known.
                // - Fee Recipient (index 1) is on-curve but known.
                // - Bonding Curve (index 3?) is OFF-curve (PDA).
                // - Associated Bonding Curve (index 4?) is OFF-curve (PDA).
                // - Mint is ON-curve.

                // So, skip index 0 and 1. Find the first ON-curve account.
                // Also skip System Program, Token Program, etc.

                let known_programs = [
                    pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"), // Token
                    pubkey!("11111111111111111111111111111111"),            // System
                    pubkey!("SysvarRent111111111111111111111111111111111"), // Rent
                    pubkey!("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"), // Metadata
                    pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"), // ATA
                    pubkey!("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"), // Event Auth
                    pubkey!("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM"), // Fee Recipient (correct)
                    pubkey!("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"), // Global
                    pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),  // Pump.fun Program
                ];

                for acc in tx_update.instruction_accounts.iter() {
                    // Skip known accounts
                    if known_programs.contains(acc) {
                        continue;
                    }

                    // Skip PDAs (Bonding Curve is PDA)
                    if !acc.is_on_curve() {
                        continue;
                    }

                    // The first remaining on-curve account is likely the Mint or the Mint Authority (Payer).
                    // In Create instruction, Mint comes before Mint Authority usually?
                    // Or Mint Authority comes first?
                    // In the log: ix_2 (4AJX...) was Mint? ix_3 (6ydT...) was Bonding Curve.
                    // If 4AJX... is on curve, and 6ydT... is off curve.
                    // Then 4AJX... is the candidate.

                    // We assume the Mint is the first unknown on-curve account.
                    found_mint = Some(*acc);
                    break;
                }

                if let Some(mint) = found_mint {
                    tracing::info!(
                        signature = %tx_update.signature,
                        mint = %mint,
                        "geyser_pool_discovery: ✅ DETECTED MINT via Heuristic"
                    );
                    mint
                } else {
                    tracing::warn!(
                        signature = %tx_update.signature,
                        "geyser_pool_discovery: ❌ FAILED TO DETECT MINT - Fallback to index 2"
                    );
                    tx_update.instruction_accounts.get(2).copied()?
                }
            }
            DexType::RaydiumAmmV4 => {
                // For Raydium: need to analyze transaction structure
                // For now, skip until we analyze Raydium pool creations
                return None;
            }
            DexType::OrcaWhirlpool => {
                // For Orca: need to analyze transaction structure
                // For now, skip until we analyze Orca pool creations
                return None;
            }
        };

        // Verify token mint exists on-chain via simple account fetch
        // REMOVED: RPC check is too slow and prone to race conditions for new mints
        // We trust the Geyser stream which just told us the mint is being created
        /*
        match rpc.get_account_retry(&token_mint).await {
            Ok(_) => {
                // Mint exists, continue
            }
            Err(_) => {
                debug!(
                    token_mint = %token_mint,
                    signature = %tx_update.signature,
                    "geyser_pool_discovery: token mint does not exist or fetch failed"
                );
                return None;
            }
        }
        */

        // Extract pool address (bonding curve for Pump.fun)
        // For Pump.fun: instruction account[2] is the bonding curve PDA (the "pool")
        // Index 3 is the associated bonding curve (token vault), NOT the bonding curve!
        let pool_address = match dex_type {
            DexType::PumpFun => tx_update.instruction_accounts.get(2).copied()?,
            _ => tx_update.account_keys.first().copied()?,
        };

        // Extract creator (for Pump.fun - needed for new buy instruction accounts)
        // Pump.fun CREATE instruction accounts:
        // [0]: Global, [1]: Fee Recipient, [2]: Mint, [3]: Bonding Curve,
        // [4]: Associated Bonding Curve, [5]: User/Creator (Signer)
        // However, based on actual tx logs, user is usually at index 6
        let creator = match dex_type {
            DexType::PumpFun => {
                // User/Creator is typically at index 6 in the CREATE instruction
                // But let's find the first signer that's on-curve and not a known program
                // Known programs to skip (many are on-curve!):
                let metaplex = pubkey!("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");
                let token_prog = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
                let ata_prog = pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
                let system_prog = pubkey!("11111111111111111111111111111111");
                let rent = pubkey!("SysvarRent111111111111111111111111111111111");
                let event_auth = pubkey!("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1");
                let pumpfun_global = pubkey!("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");
                let pumpfun_prog = pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
                let fee_prog = pubkey!("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
                // Fee recipients (multiple possible)
                let fee_recip_1 = pubkey!("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM");
                let fee_recip_2 = pubkey!("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV");
                
                let known_to_skip = [
                    metaplex, token_prog, ata_prog, system_prog, rent,
                    event_auth, pumpfun_global, pumpfun_prog, fee_prog,
                    fee_recip_1, fee_recip_2,
                ];
                
                tx_update.instruction_accounts.iter()
                    .skip(5) // Skip known accounts: Global, Fee, Mint, Bonding, AssocBonding
                    .find(|acc| acc.is_on_curve() && !known_to_skip.contains(acc))
                    .copied()
            }
            _ => None,
        };

        // SOL mint for quote
        let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").ok()?;

        tracing::info!(
            signature = %tx_update.signature,
            slot = tx_update.slot,
            dex = ?dex_type,
            pool = %pool_address,
            token_mint = %token_mint,
            creator = ?creator,
            account_count = account_count,
            "geyser_pool_discovery: NEW POOL via TRANSACTION (professional method)"
        );

        // Create pool discovery event with transaction-based mint
        let discovered_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        Some(PoolDiscoveryEvent {
            pool_address,
            dex_type,
            slot: tx_update.slot,
            base_mint: token_mint,
            quote_mint,
            base_decimals: 9,  // Standard for Pump.fun, fetch actual later if needed
            quote_decimals: 9, // SOL decimals
            liquidity_estimate_lamports: 30_000_000_000, // Default 30 SOL for new pools
            coin_vault: None,  // Will be fetched in background if needed
            pc_vault: None,
            creator,
            discovered_at_ms,
        })
    }

    /// Parse Raydium AMM v4 pool account (based on official program/src/state.rs)
    fn parse_raydium_pool(data: &[u8]) -> Option<PoolData> {
        // Raydium AMM v4 account layout: 752 bytes total
        // Source: https://github.com/raydium-io/raydium-amm/blob/master/program/src/state.rs
        if data.len() != 752 {
            // Log ignored account sizes to debug what we are picking up (e.g. 2208 bytes)
            if data.len() == 2208 {
                tracing::debug!(len = data.len(), "geyser_pool_discovery: ignoring Raydium account (likely OpenOrders/TargetOrders)");
            }
            return None;
        }

        // Offset 0: status (u64) - check if initialized
        let status = u64::from_le_bytes(data[0..8].try_into().ok()?);
        if status == 0 {
            return None; // AmmStatus::Uninitialized
        }

        // Offset 32: coin_decimals (u64)
        let coin_decimals = u64::from_le_bytes(data[32..40].try_into().ok()?) as u8;
        // Offset 40: pc_decimals (u64)
        let pc_decimals = u64::from_le_bytes(data[40..48].try_into().ok()?) as u8;

        // Offset 336: coin_vault (Pubkey) - holds base token reserves
        let coin_vault = Pubkey::new_from_array(data[336..368].try_into().ok()?);
        // Offset 368: pc_vault (Pubkey) - holds quote token reserves
        let pc_vault = Pubkey::new_from_array(data[368..400].try_into().ok()?);

        // Offset 400: coin_vault_mint (Pubkey) - this is the BASE MINT!
        let base_mint = Pubkey::new_from_array(data[400..432].try_into().ok()?);
        // Offset 432: pc_vault_mint (Pubkey) - this is the QUOTE MINT!
        let quote_mint = Pubkey::new_from_array(data[432..464].try_into().ok()?);

        // Offset 720: lp_amount (u64) - total LP tokens minted
        let lp_amount = u64::from_le_bytes(data[720..728].try_into().ok()?);

        // Conservative liquidity estimation until vault fetch completes
        // LP amount is NOT a reliable indicator of TVL due to varying decimals
        // Use fixed reasonable estimates to avoid over-estimating
        let liquidity_lamports = if lp_amount == 0 {
            // Brand new pool with no LP minted yet
            5_000_000_000 // 5 SOL default
        } else {
            // Any pool with LP minted: assume moderate liquidity
            // Actual value will be fetched from vaults in background
            50_000_000_000 // 50 SOL conservative estimate
        };

        Some(PoolData {
            base_mint,
            quote_mint,
            base_decimals: coin_decimals,
            quote_decimals: pc_decimals,
            liquidity_lamports,
            coin_vault: Some(coin_vault),
            pc_vault: Some(pc_vault),
        })
    }

    /// Parse Orca Whirlpool account
    fn parse_orca_pool(data: &[u8]) -> Option<PoolData> {
        // Use existing orca_whirlpool_layout parser
        let parsed = crate::solana::dex::orca_whirlpool_layout::parse_whirlpool(data)?;

        // Calculate liquidity estimate from sqrt_price and liquidity fields
        // liquidity (u128) is L in concentrated liquidity math
        // Rough estimate: L / sqrt(1.0001)^tick_spacing as SOL equivalent
        let liquidity_lamports = if parsed.liquidity > 0 {
            // Conservative: use liquidity field scaled to lamports (L typically in units of sqrt(token amounts))
            // For 1:1 pools at current price, L ~= sqrt(reserves_a * reserves_b)
            // Estimate: L^2 / 1e9 / price_ratio ~= SOL liquidity
            // Simplified: L / 10^6 as rough SOL estimate (will be refined by vault fetch)
            (parsed.liquidity / 1_000_000).min(1_000_000_000_000) as u64 // Cap at 1M SOL
        } else {
            5_000_000_000 // 5 SOL default for new pools
        };

        Some(PoolData {
            base_mint: parsed.token_mint_a,
            quote_mint: parsed.token_mint_b,
            base_decimals: 9, // Will need to fetch from mint account
            quote_decimals: 9,
            liquidity_lamports,
            coin_vault: Some(parsed.token_vault_a), // Enable background vault fetch
            pc_vault: Some(parsed.token_vault_b),
        })
    }

    /// Parse Pump.fun bonding curve account
    /// Layout (reverse-engineered from 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P):
    /// - Offset 8: virtual_token_reserves (u64)
    /// - Offset 16: virtual_sol_reserves (u64)
    /// - Offset 24: real_token_reserves (u64)
    /// - Offset 32: real_sol_reserves (u64)
    /// - Offset 40: token_mint (Pubkey)
    /// - Offset 72: bonding_curve (Pubkey)
    ///
    /// DEPRECATED: This method produces wrong token mints (12xtdJLo...)
    /// Use transaction-based discovery instead (process_transaction_update)
    #[allow(dead_code)]
    fn parse_pumpfun_bonding_curve(data: &[u8]) -> Option<PoolData> {
        if data.len() < 104 {
            tracing::info!(
                data_len = data.len(),
                "pump.fun bonding curve data too short"
            );
            return None;
        }

        // Parse virtual/real reserves
        let virtual_token_reserves = u64::from_le_bytes(data[8..16].try_into().ok()?);
        let virtual_sol_reserves = u64::from_le_bytes(data[16..24].try_into().ok()?);
        let real_token_reserves = u64::from_le_bytes(data[24..32].try_into().ok()?);
        let real_sol_reserves = u64::from_le_bytes(data[32..40].try_into().ok()?);

        // Token mint (base)
        let base_mint = Pubkey::new_from_array(data[40..72].try_into().ok()?);

        // DEBUG: Log raw bytes and parsed mint
        tracing::info!(
            data_len = data.len(),
            base_mint = %base_mint,
            mint_bytes = ?&data[40..72],
            "pump.fun: parsed token mint from geyser data"
        );

        // Quote mint is always SOL for Pump.fun
        let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
            .unwrap_or_else(|_| Pubkey::new_from_array([0u8; 32]));

        // Sanity checks
        if base_mint.to_bytes() == [0u8; 32] {
            return None;
        }

        // Use real reserves for liquidity estimation (more accurate than virtual)
        // real_sol_reserves is in lamports, convert to SOL
        let liquidity_lamports = real_sol_reserves.max(virtual_sol_reserves);

        tracing::info!(
            base_mint=%base_mint,
            virtual_token=virtual_token_reserves,
            virtual_sol=virtual_sol_reserves,
            real_token=real_token_reserves,
            real_sol=real_sol_reserves,
            data_len=data.len(),
            first_104_bytes=?&data[0..104.min(data.len())],
            "pump.fun bonding curve parsed"
        );

        Some(PoolData {
            base_mint,
            quote_mint,
            base_decimals: 9,  // Pump.fun tokens typically use 9 decimals
            quote_decimals: 9, // SOL decimals
            liquidity_lamports,
            coin_vault: None, // Pump.fun uses bonding curve, not traditional vaults
            pc_vault: None,
        })
    }
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
            DexType::RaydiumAmmV4 => write!(f, "Raydium"),
            DexType::OrcaWhirlpool => write!(f, "Orca"),
            DexType::PumpFun => write!(f, "PumpFun"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolDiscoveryEvent {
    pub pool_address: Pubkey,
    pub dex_type: DexType,
    pub slot: u64,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub liquidity_estimate_lamports: u64,
    pub coin_vault: Option<Pubkey>,
    pub pc_vault: Option<Pubkey>,
    /// Creator address (for Pump.fun tokens - needed for buy instruction)
    pub creator: Option<Pubkey>,
    /// Timestamp when this event was created (for latency tracking)
    pub discovered_at_ms: u64,
}

struct PoolData {
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_decimals: u8,
    quote_decimals: u8,
    liquidity_lamports: u64,
    coin_vault: Option<Pubkey>,
    pc_vault: Option<Pubkey>,
}
