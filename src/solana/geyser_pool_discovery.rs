//! Professional Geyser-based Pool Discovery
//! Replaces WebSocket log parsing with structured account updates

use crate::solana::geyser_listener::{GeyserAccountUpdate, GeyserListener};
use crate::solana::rpc::SolanaRpc;
use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

/// Pool discovery via Geyser account updates
pub struct GeyserPoolDiscovery {
    listener: GeyserListener,
    rpc: Arc<SolanaRpc>,
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
        let (listener, account_rx) = GeyserListener::new(geyser_endpoint, program_ids);

        // Create event channel for pool discoveries
        let (event_tx, event_rx) = broadcast::channel(10000);

        let rpc_clone = rpc.clone();
        let discovery = Self {
            listener,
            rpc: rpc_clone,
        };

        // Spawn account processor
        let rpc_clone2 = rpc.clone();
        tokio::spawn(async move {
            let mut rx = account_rx;
            while let Ok(update) = rx.recv().await {
                if let Some(event) = Self::process_account_update(update, &rpc_clone2).await {
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
        rpc: &Arc<SolanaRpc>,
    ) -> Option<PoolDiscoveryEvent> {
        // Identify DEX by owner
        let dex_type = match update.owner.to_string().as_str() {
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" => DexType::RaydiumAmmV4,
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" => DexType::OrcaWhirlpool,
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" => DexType::PumpFun,
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
            DexType::PumpFun => Self::parse_pumpfun_bonding_curve(&update.data),
        }?;

        info!(
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
        })
    }

    /// Parse Raydium AMM v4 pool account
    fn parse_raydium_pool(data: &[u8]) -> Option<PoolData> {
        // Raydium AMM v4 layout (simplified - adjust offsets as needed)
        if data.len() < 752 {
            return None;
        }

        // Check discriminator (first 8 bytes should match pool account type)
        // Adjust based on actual Raydium layout
        let status = data[0];
        if status == 0 {
            // Not initialized
            return None;
        }

        // Parse base/quote mints (adjust offsets based on Raydium struct)
        let base_mint = Pubkey::new_from_array(data[8..40].try_into().ok()?);
        let quote_mint = Pubkey::new_from_array(data[40..72].try_into().ok()?);

        // Parse decimals
        let base_decimals = data[72];
        let quote_decimals = data[73];

        // Parse base/quote reserves to estimate liquidity
        let _base_reserve = u64::from_le_bytes(data[200..208].try_into().ok()?);
        let quote_reserve = u64::from_le_bytes(data[208..216].try_into().ok()?);

        // Estimate total liquidity in lamports (assuming quote is SOL or USDC)
        let liquidity_lamports = quote_reserve * 2; // rough estimate

        Some(PoolData {
            base_mint,
            quote_mint,
            base_decimals,
            quote_decimals,
            liquidity_lamports,
        })
    }

    /// Parse Orca Whirlpool account
    fn parse_orca_pool(data: &[u8]) -> Option<PoolData> {
        // Use existing orca_whirlpool_layout parser
        let parsed = crate::solana::dex::orca_whirlpool_layout::parse_whirlpool(data)?;

        Some(PoolData {
            base_mint: parsed.token_mint_a,
            quote_mint: parsed.token_mint_b,
            base_decimals: 9, // Will need to fetch from mint account
            quote_decimals: 9,
            liquidity_lamports: 0, // Calculate from vault balances
        })
    }

    /// Parse Pump.fun bonding curve account
    fn parse_pumpfun_bonding_curve(data: &[u8]) -> Option<PoolData> {
        // Pump.fun bonding curve layout (needs reverse engineering)
        // For now, return None - will implement after getting actual layout
        if data.len() < 200 {
            return None;
        }

        // Placeholder - needs actual Pump.fun struct layout
        warn!("pump.fun parsing not yet implemented - needs bonding curve layout");
        None
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
}

struct PoolData {
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_decimals: u8,
    quote_decimals: u8,
    liquidity_lamports: u64,
}
