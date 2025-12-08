//! Professional Geyser-based Pool Discovery
//! Replaces WebSocket log parsing with structured account updates

use crate::solana::geyser_listener::{GeyserAccountUpdate, GeyserListener, GeyserTransactionUpdate};
use crate::solana::rpc::SolanaRpc;
use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::str::FromStr;
use tokio::sync::broadcast;
use tracing::{debug, info};

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
        let (listener, account_rx, transaction_rx) = GeyserListener::new(geyser_endpoint, program_ids);

        // Create event channel for pool discoveries
        let (event_tx, event_rx) = broadcast::channel(10000);

        let discovery = Self { listener };

        // Spawn account processor
        let rpc_clone = rpc.clone();
        let event_tx_clone = event_tx.clone();
        tokio::spawn(async move {
            let mut rx = account_rx;
            while let Ok(update) = rx.recv().await {
                if let Some(event) = Self::process_account_update(update, &rpc_clone).await {
                    let _ = event_tx_clone.send(event);
                }
            }
        });

        // Spawn transaction processor
        let rpc_clone2 = rpc.clone();
        tokio::spawn(async move {
            let mut rx = transaction_rx;
            while let Ok(tx_update) = rx.recv().await {
                if let Some(event) = Self::process_transaction_update(tx_update, &rpc_clone2).await {
                    let _ = event_tx.send(event);
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
        })
    }

    /// Process transaction update to extract token mints from instruction accounts
    /// This is the professional approach - transaction instructions contain token mints
    /// as instruction accounts, avoiding the need to parse account data layouts
    async fn process_transaction_update(
        tx_update: GeyserTransactionUpdate,
        rpc: &Arc<SolanaRpc>,
    ) -> Option<PoolDiscoveryEvent> {
        // Identify DEX by looking for known program IDs in account_keys
        let raydium_program = Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").ok()?;
        let orca_program = Pubkey::from_str("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc").ok()?;
        let pumpfun_program = Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").ok()?;
        
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
        // Buy/Sell transactions have 18 accounts, pool creations have 16 or 23
        let account_count = tx_update.account_keys.len();
        if dex_type == DexType::PumpFun && account_count == 18 {
            // This is a Buy/Sell transaction, not a pool creation - ignore
            return None;
        }
        
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
            tracing::info!(
                message = "DEBUG: Pump.fun CREATE transaction account_keys",
                account_count = tx_update.account_keys.len(),
                account_0 = %tx_update.account_keys.get(0).map(|k| k.to_string()).unwrap_or_else(|| "None".to_string()),
                account_1 = %tx_update.account_keys.get(1).map(|k| k.to_string()).unwrap_or_else(|| "None".to_string()),
                account_2 = %tx_update.account_keys.get(2).map(|k| k.to_string()).unwrap_or_else(|| "None".to_string()),
                account_3 = %tx_update.account_keys.get(3).map(|k| k.to_string()).unwrap_or_else(|| "None".to_string()),
                account_4 = %tx_update.account_keys.get(4).map(|k| k.to_string()).unwrap_or_else(|| "None".to_string()),
                account_5 = %tx_update.account_keys.get(5).map(|k| k.to_string()).unwrap_or_else(|| "None".to_string()),
            );
        }
        
        let token_mint = match dex_type {
            DexType::PumpFun => {
                // TEMPORARY: Using account[0] but need to verify with debug logs
                // Will fix once we see actual account_keys structure
                tx_update.account_keys.get(0).copied()?
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
        
        // Extract pool address (bonding curve for Pump.fun)
        // For Pump.fun: account[2] is the bonding curve PDA (the "pool")
        // account[3] is the associated bonding curve vault
        let pool_address = match dex_type {
            DexType::PumpFun => tx_update.account_keys.get(2).copied()?,
            _ => tx_update.account_keys.get(0).copied()?,
        };
        
        // SOL mint for quote
        let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").ok()?;
        
        debug!(
            signature = %tx_update.signature,
            slot = tx_update.slot,
            dex = ?dex_type,
            pool = %pool_address,
            token_mint = %token_mint,
            account_count = account_count,
            "geyser_pool_discovery: NEW POOL via TRANSACTION (professional method)"
        );
        
        // Create pool discovery event with transaction-based mint
        Some(PoolDiscoveryEvent {
            pool_address,
            dex_type,
            slot: tx_update.slot,
            base_mint: token_mint,
            quote_mint,
            base_decimals: 9, // Standard for Pump.fun, fetch actual later if needed
            quote_decimals: 9, // SOL decimals
            liquidity_estimate_lamports: 30_000_000_000, // Default 30 SOL for new pools
            coin_vault: None, // Will be fetched in background if needed
            pc_vault: None,
        })
    }

    /// Parse Raydium AMM v4 pool account (based on official program/src/state.rs)
    fn parse_raydium_pool(data: &[u8]) -> Option<PoolData> {
        // Raydium AMM v4 account layout: 752 bytes total
        // Source: https://github.com/raydium-io/raydium-amm/blob/master/program/src/state.rs
        if data.len() < 752 {
            return None;
        }

        // Offset 0: status (u64) - check if initialized
        let status = u64::from_le_bytes(data[0..8].try_into().ok()?);
        if status == 0 {
            return None; // AmmStatus::Uninitialized
        }

        // Offset 40: coin_decimals (u64)
        let coin_decimals = u64::from_le_bytes(data[40..48].try_into().ok()?) as u8;
        // Offset 48: pc_decimals (u64)
        let pc_decimals = u64::from_le_bytes(data[48..56].try_into().ok()?) as u8;

        // Offset 400: coin_vault (Pubkey) - holds base token reserves
        let coin_vault = Pubkey::new_from_array(data[400..432].try_into().ok()?);
        // Offset 432: pc_vault (Pubkey) - holds quote token reserves
        let pc_vault = Pubkey::new_from_array(data[432..464].try_into().ok()?);

        // Offset 464: coin_vault_mint (Pubkey) - this is the BASE MINT!
        let base_mint = Pubkey::new_from_array(data[464..496].try_into().ok()?);
        // Offset 496: pc_vault_mint (Pubkey) - this is the QUOTE MINT!
        let quote_mint = Pubkey::new_from_array(data[496..528].try_into().ok()?);

        // Offset 688: lp_amount (u64) - total LP tokens minted
        let lp_amount = u64::from_le_bytes(data[688..696].try_into().ok()?);
        
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
            base_decimals: 9, // Pump.fun tokens typically use 9 decimals
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
