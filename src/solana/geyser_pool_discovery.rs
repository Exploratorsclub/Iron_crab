//! Professional Geyser-based Pool Discovery
//! Simplified approach: TX-based for Pump.fun, Account-based for Raydium/Orca

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

        // Spawn account processor (for Raydium/Orca)
        let rpc_clone = rpc.clone();
        let event_tx_clone = event_tx.clone();
        tokio::spawn(async move {
            let mut rx = account_rx;
            loop {
                match rx.recv().await {
                    Ok(update) => {
                        if let Some(event) = Self::process_account_update(update, &rpc_clone).await
                        {
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

        // Spawn transaction processor (for Pump.fun - simplified approach)
        let rpc_clone2 = rpc;
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
    /// Used for Raydium/Orca (Pump.fun uses TX-based discovery)
    async fn process_account_update(
        update: GeyserAccountUpdate,
        _rpc: &Arc<SolanaRpc>,
    ) -> Option<PoolDiscoveryEvent> {
        // Identify DEX by owner
        let dex_type = match update.owner.to_string().as_str() {
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" => DexType::RaydiumAmmV4,
            "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C" => DexType::RaydiumCpmm,
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" => DexType::OrcaWhirlpool,
            "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo" => DexType::MeteoraDlmm,
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" => {
                // Pump.fun: TX-based discovery is faster and simpler
                // We don't need account updates for Pump.fun anymore
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
            DexType::RaydiumCpmm => Self::parse_raydium_cpmm_pool(&update.data),
            DexType::OrcaWhirlpool => Self::parse_orca_pool(&update.data),
            DexType::MeteoraDlmm => Self::parse_meteora_dlmm_pool(&update.data),
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
    /// For Pump.fun: SIMPLIFIED - mint from TX, bonding curve PDA calculated, creator = signer
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

        // Process based on DEX type
        match dex_type {
            DexType::PumpFun => {
                // SIMPLIFIED APPROACH (per Sniper Playbook):
                // 1. Get Mint from TX (instruction account[0])
                // 2. CALCULATE Bonding Curve PDA from mint
                // 3. Creator = instruction account[7] (User)

                // Need at least 8 instruction accounts for CREATE
                if tx_update.instruction_accounts.len() < 8 {
                    return None;
                }

                // Check for CREATE discriminator
                const PUMPFUN_CREATE_DISCRIMINATOR: [u8; 8] =
                    [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];

                if tx_update.instruction_data.len() < 8 {
                    return None;
                }
                let discriminator = &tx_update.instruction_data[0..8];
                if discriminator != PUMPFUN_CREATE_DISCRIMINATOR {
                    return None; // Not CREATE, likely BUY or SELL
                }

                // Pump.fun CREATE instruction accounts (from Solscan):
                // [0]: Mint (the token being created!) ← THIS IS THE MINT
                // [1]: Mint Authority
                // [2]: Bonding Curve
                // [3]: Associated Bonding Curve (vault)
                // [4]: Global
                // [5]: Metaplex Token Metadata Program
                // [6]: Metadata account
                // [7]: User (creator, fee payer) ← THIS IS THE CREATOR

                // Get the mint (account index 0)
                let token_mint = *tx_update.instruction_accounts.first()?;

                // CALCULATE Bonding Curve PDA deterministically
                // Seeds: ["bonding-curve", mint_pubkey]
                let (bonding_curve, _bump) = Pubkey::find_program_address(
                    &[b"bonding-curve", token_mint.as_ref()],
                    &pumpfun_program,
                );

                // Creator = User at instruction account[7]
                let creator = tx_update.instruction_accounts.get(7).copied();

                let quote_mint = pubkey!("So11111111111111111111111111111111111111112");

                let discovered_at_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                tracing::info!(
                    signature = %tx_update.signature,
                    slot = tx_update.slot,
                    mint = %token_mint,
                    bonding_curve = %bonding_curve,
                    creator = ?creator,
                    "geyser_pool_discovery: PUMPFUN CREATE (simplified: PDA calculated, creator from signer)"
                );

                Some(PoolDiscoveryEvent {
                    pool_address: bonding_curve,
                    dex_type: DexType::PumpFun,
                    slot: tx_update.slot,
                    base_mint: token_mint,
                    quote_mint,
                    base_decimals: 6,  // Pump.fun uses 6 decimals
                    quote_decimals: 9, // SOL
                    liquidity_estimate_lamports: 30_000_000_000, // 30 SOL default
                    coin_vault: None,
                    pc_vault: None,
                    creator,
                    discovered_at_ms,
                })
            }
            DexType::RaydiumAmmV4 | DexType::RaydiumCpmm | DexType::OrcaWhirlpool | DexType::MeteoraDlmm => {
                // Raydium/Orca/Meteora: TX-based discovery not implemented yet
                // We use account-based discovery for these (more efficient)
                None
            }
        }
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

    /// Parse Raydium CPMM pool account (1024 bytes)
    /// Source: Raydium CPMM program (CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C)
    fn parse_raydium_cpmm_pool(data: &[u8]) -> Option<PoolData> {
        // CPMM pool account size: 1024 bytes (verified from mainnet)
        if data.len() != 1024 {
            tracing::debug!(len = data.len(), "geyser_pool_discovery: ignoring CPMM account (wrong size)");
            return None;
        }

        // Offset 0: discriminator (8 bytes)
        // Offset 8: status (u8) - 0 = uninitialized, 1 = initialized, 6 = disabled
        let status = data[8];
        if status == 0 {
            tracing::debug!("geyser_pool_discovery: CPMM pool uninitialized (status=0)");
            return None;
        }

        // Offset 73: token_0_mint (Pubkey 32 bytes)
        let token_0_mint = Pubkey::new_from_array(data[73..105].try_into().ok()?);
        // Offset 105: token_1_mint (Pubkey 32 bytes)
        let token_1_mint = Pubkey::new_from_array(data[105..137].try_into().ok()?);

        // Offset 137: token_0_vault (Pubkey 32 bytes)
        let token_0_vault = Pubkey::new_from_array(data[137..169].try_into().ok()?);
        // Offset 169: token_1_vault (Pubkey 32 bytes)
        let token_1_vault = Pubkey::new_from_array(data[169..201].try_into().ok()?);

        // Sanity check: mints should not be zero
        if token_0_mint.to_bytes() == [0u8; 32] || token_1_mint.to_bytes() == [0u8; 32] {
            tracing::debug!("geyser_pool_discovery: CPMM pool has zero mints");
            return None;
        }

        // Determine which is base and which is quote (SOL is usually quote)
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
            .unwrap_or_else(|_| Pubkey::new_from_array([0u8; 32]));

        let (base_mint, quote_mint, coin_vault, pc_vault) = if token_1_mint == sol_mint {
            // Token 1 is SOL (quote), Token 0 is base
            (token_0_mint, token_1_mint, token_0_vault, token_1_vault)
        } else if token_0_mint == sol_mint {
            // Token 0 is SOL (quote), Token 1 is base
            (token_1_mint, token_0_mint, token_1_vault, token_0_vault)
        } else {
            // Neither is SOL, use token_0 as base by convention
            (token_0_mint, token_1_mint, token_0_vault, token_1_vault)
        };

        // Conservative liquidity estimate (will be fetched from vaults in background)
        let liquidity_lamports = 50_000_000_000; // 50 SOL default

        tracing::info!(
            base_mint=%base_mint,
            quote_mint=%quote_mint,
            status=status,
            "geyser_pool_discovery: CPMM pool parsed"
        );

        Some(PoolData {
            base_mint,
            quote_mint,
            base_decimals: 9, // Will be fetched from mint
            quote_decimals: 9,
            liquidity_lamports,
            coin_vault: Some(coin_vault),
            pc_vault: Some(pc_vault),
        })
    }

    /// Parse Meteora DLMM pool account (LB Pair - 904 bytes)
    /// Source: Meteora DLMM program (LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo)
    fn parse_meteora_dlmm_pool(data: &[u8]) -> Option<PoolData> {
        // Meteora DLMM LB Pair account size: 904 bytes
        if data.len() != 904 {
            tracing::debug!(len = data.len(), "geyser_pool_discovery: ignoring Meteora DLMM account (wrong size)");
            return None;
        }

        // Use existing parser from meteora_dlmm_layout
        let parsed = crate::solana::dex::meteora_dlmm_layout::DlmmPool::parse(data).ok()?;

        // Sanity check: mints should not be zero
        if parsed.token_x_mint.to_bytes() == [0u8; 32] || parsed.token_y_mint.to_bytes() == [0u8; 32] {
            tracing::debug!("geyser_pool_discovery: Meteora DLMM pool has zero mints");
            return None;
        }

        // Determine which is base and which is quote (SOL is usually quote)
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
            .unwrap_or_else(|_| Pubkey::new_from_array([0u8; 32]));

        let (base_mint, quote_mint, coin_vault, pc_vault) = if parsed.token_y_mint == sol_mint {
            // Token Y is SOL (quote), Token X is base
            (parsed.token_x_mint, parsed.token_y_mint, parsed.reserve_x, parsed.reserve_y)
        } else if parsed.token_x_mint == sol_mint {
            // Token X is SOL (quote), Token Y is base
            (parsed.token_y_mint, parsed.token_x_mint, parsed.reserve_y, parsed.reserve_x)
        } else {
            // Neither is SOL, use token_x as base by convention
            (parsed.token_x_mint, parsed.token_y_mint, parsed.reserve_x, parsed.reserve_y)
        };

        // Estimate liquidity from bin parameters
        // liquidity (u128) represents concentrated liquidity at active bin
        let liquidity_lamports = if parsed.active_id > 0 && parsed.bin_step > 0 {
            // Conservative estimate based on active bin liquidity
            // Real reserves will be fetched from vaults in background
            (parsed.active_id as u64).saturating_mul(1_000_000).min(100_000_000_000) // Cap at 100 SOL
        } else {
            5_000_000_000 // 5 SOL default
        };

        tracing::info!(
            token_x=%parsed.token_x_mint,
            token_y=%parsed.token_y_mint,
            active_id=parsed.active_id,
            bin_step=parsed.bin_step,
            "geyser_pool_discovery: Meteora DLMM pool parsed"
        );

        Some(PoolData {
            base_mint,
            quote_mint,
            base_decimals: 9, // Will be fetched from mint
            quote_decimals: 9,
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
    RaydiumCpmm,
    OrcaWhirlpool,
    MeteoraDlmm,
    PumpFun,
}

impl std::fmt::Display for DexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // IMPORTANT: These names must match:
        // 1. is_known_dex_label() in arb_strategy.rs
        // 2. DEX connector keys in cross_dex_handler.rs
        match self {
            DexType::RaydiumAmmV4 => write!(f, "raydium"),
            DexType::RaydiumCpmm => write!(f, "raydium_cpmm"),
            DexType::OrcaWhirlpool => write!(f, "orca"),
            DexType::MeteoraDlmm => write!(f, "meteora_dlmm"),
            DexType::PumpFun => write!(f, "pumpfun"),
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
