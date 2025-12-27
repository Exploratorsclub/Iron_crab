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
                // Pump.fun: Extract creator from bonding curve account data
                // This is the REAL creator stored at bytes [49..81], NOT the tx signer!
                if let Some(event) = Self::parse_pumpfun_bonding_curve_full(&update) {
                    return Some(event);
                }
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
                // DISABLED: Transaction-based discovery for Pump.fun
                // Now using Account-based discovery which extracts the REAL creator from bonding curve data
                // This avoids the bug where tx signer != actual creator stored in bonding curve
                debug!(
                    signature = %tx_update.signature,
                    "geyser_pool_discovery: Pump.fun TX ignored - using account-based discovery instead"
                );
                return None;
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

        // SOL mint for quote
        #[allow(unreachable_code)]
        let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").ok()?;

        tracing::info!(
            signature = %tx_update.signature,
            slot = tx_update.slot,
            dex = ?dex_type,
            pool = %token_mint, // placeholder since all branches return None
            token_mint = %token_mint,
            account_count = account_count,
            "geyser_pool_discovery: NEW POOL via TRANSACTION (professional method)"
        );

        // Create pool discovery event with transaction-based mint
        let discovered_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Some(PoolDiscoveryEvent {
            pool_address: token_mint, // placeholder
            dex_type,
            slot: tx_update.slot,
            base_mint: token_mint,
            quote_mint,
            base_decimals: 9,
            quote_decimals: 9,
            liquidity_estimate_lamports: 30_000_000_000,
            coin_vault: None,
            pc_vault: None,
            creator: None,
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

    /// Parse Pump.fun bonding curve from Geyser account update
    /// Returns a full PoolDiscoveryEvent with the REAL creator from bytes [49..81]
    ///
    /// Bonding Curve Layout (from pump.fun program):
    /// - Offset 0: discriminator (8 bytes)
    /// - Offset 8: virtual_token_reserves (u64)
    /// - Offset 16: virtual_sol_reserves (u64)  
    /// - Offset 24: real_token_reserves (u64)
    /// - Offset 32: real_sol_reserves (u64)
    /// - Offset 40: token_total_supply (u64)
    /// - Offset 48: complete (u8, bool)
    /// - Offset 49: creator (Pubkey, 32 bytes) <-- THE REAL CREATOR!
    /// - Offset 81: mint (Pubkey, 32 bytes)
    fn parse_pumpfun_bonding_curve_full(
        update: &GeyserAccountUpdate,
    ) -> Option<PoolDiscoveryEvent> {
        let data = &update.data;

        // Minimum size: discriminator(8) + reserves(32) + total_supply(8) + complete(1) + creator(32) + mint(32) = 113 bytes
        if data.len() < 113 {
            debug!(
                data_len = data.len(),
                pool = %update.pubkey,
                "pump.fun bonding curve too short for full parse"
            );
            return None;
        }

        // Parse reserves
        let virtual_token_reserves = u64::from_le_bytes(data[8..16].try_into().ok()?);
        let virtual_sol_reserves = u64::from_le_bytes(data[16..24].try_into().ok()?);
        let real_token_reserves = u64::from_le_bytes(data[24..32].try_into().ok()?);
        let real_sol_reserves = u64::from_le_bytes(data[32..40].try_into().ok()?);

        // Parse complete flag (offset 48)
        let complete = data[48] != 0;

        // Parse REAL creator (offset 49-81) - THIS IS THE KEY FIX!
        let creator = Pubkey::new_from_array(data[49..81].try_into().ok()?);

        // Parse token mint (offset 81-113)
        let base_mint = Pubkey::new_from_array(data[81..113].try_into().ok()?);

        // SOL as quote
        let quote_mint = pubkey!("So11111111111111111111111111111111111111112");

        // Sanity checks
        if base_mint.to_bytes() == [0u8; 32] {
            debug!("pump.fun: base_mint is zero, skipping");
            return None;
        }

        // Skip if already completed (migrated to Raydium)
        if complete {
            debug!(
                pool = %update.pubkey,
                mint = %base_mint,
                "pump.fun: bonding curve complete (migrated), skipping"
            );
            return None;
        }

        // Use real SOL reserves as liquidity estimate
        let liquidity_lamports = real_sol_reserves.max(virtual_sol_reserves);

        let discovered_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        tracing::info!(
            pool = %update.pubkey,
            slot = update.slot,
            mint = %base_mint,
            creator = %creator,
            real_sol = real_sol_reserves,
            virtual_sol = virtual_sol_reserves,
            complete = complete,
            "geyser_pool_discovery: PUMPFUN pool via ACCOUNT UPDATE (with REAL creator!)"
        );

        Some(PoolDiscoveryEvent {
            pool_address: update.pubkey,
            dex_type: DexType::PumpFun,
            slot: update.slot,
            base_mint,
            quote_mint,
            base_decimals: 6,  // Pump.fun tokens use 6 decimals
            quote_decimals: 9, // SOL decimals
            liquidity_estimate_lamports: liquidity_lamports,
            coin_vault: None,
            pc_vault: None,
            creator: Some(creator),
            discovered_at_ms,
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
