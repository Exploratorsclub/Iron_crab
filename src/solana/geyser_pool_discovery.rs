//! Professional Geyser-based Pool Discovery
//! Simplified approach: TX-based for Pump.fun, Account-based for Raydium/Orca
//!
//! PR162: consumes the **unified** [`crate::solana::geyser_listener::GeyserListener`] broadcast feeds
//! (no second Geyser gRPC connection).

use crate::solana::geyser_listener::{GeyserAccountUpdate, GeyserTransactionUpdate};
use crate::solana::rpc::SolanaRpc;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tracing::debug;

/// Pool discovery processors attached to the primary Geyser listener (PR162).
pub struct PoolDiscoveryIngest;

impl PoolDiscoveryIngest {
    /// Spawn account + transaction processors that emit [`PoolDiscoveryEvent`] on `mpsc`.
    ///
    /// Callers must pass [`GeyserListener::subscribe_account_updates`] /
    /// [`GeyserListener::subscribe_transaction_updates`] **before** starting the listener so no
    /// updates are missed.
    pub fn spawn_unified(
        mut account_rx: broadcast::Receiver<GeyserAccountUpdate>,
        mut transaction_rx: broadcast::Receiver<GeyserTransactionUpdate>,
        rpc: Arc<SolanaRpc>,
    ) -> mpsc::Receiver<PoolDiscoveryEvent> {
        let (event_tx, event_rx) = mpsc::channel(10_000);

        let rpc_a = rpc.clone();
        let event_tx_a = event_tx.clone();
        tokio::spawn(async move {
            loop {
                match account_rx.recv().await {
                    Ok(update) => {
                        if let Some(event) = Self::process_account_update(update, &rpc_a).await {
                            tracing::debug!(
                                dex = ?event.dex_type,
                                pool = %event.pool_address,
                                "geyser_pool_discovery: sending account-based event"
                            );
                            if event_tx_a.send(event).await.is_err() {
                                tracing::warn!("geyser_pool_discovery: pool event consumer dropped (account path)");
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "geyser_pool_discovery: account processor lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::warn!("geyser_pool_discovery: account broadcast closed");
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            loop {
                match transaction_rx.recv().await {
                    Ok(tx_update) => {
                        if let Some(event) = Self::process_transaction_update(tx_update, &rpc).await
                        {
                            tracing::info!(
                                dex = ?event.dex_type,
                                pool = %event.pool_address,
                                base_mint = %event.base_mint,
                                "geyser_pool_discovery: SENDING transaction-based event to sniper"
                            );
                            if event_tx.send(event).await.is_err() {
                                tracing::warn!(
                                    "geyser_pool_discovery: pool event consumer dropped (tx path)"
                                );
                                break;
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
                        tracing::warn!("geyser_pool_discovery: transaction broadcast closed");
                        break;
                    }
                }
            }
        });

        event_rx
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
            "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D" => DexType::MeteoraCpmm,
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
            DexType::MeteoraCpmm => Self::parse_meteora_cpmm_pool(&update.data),
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
            active_id: pool_data.active_id,
            bin_step: pool_data.bin_step,
            tick_current_index: pool_data.tick_current_index,
            tick_spacing: pool_data.tick_spacing,
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
                    active_id: None,
                    bin_step: None,
                    tick_current_index: None,
                    tick_spacing: None,
                })
            }
            DexType::RaydiumAmmV4
            | DexType::RaydiumCpmm
            | DexType::OrcaWhirlpool
            | DexType::MeteoraDlmm
            | DexType::MeteoraCpmm => {
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
            active_id: None,
            bin_step: None,
            tick_current_index: None,
            tick_spacing: None,
        })
    }

    /// Parse Raydium CPMM pool account (1024 bytes)
    /// Source: Raydium CPMM program (CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C)
    fn parse_raydium_cpmm_pool(data: &[u8]) -> Option<PoolData> {
        // CPMM pool account size: 1024 bytes (verified from mainnet)
        if data.len() != 1024 {
            tracing::debug!(
                len = data.len(),
                "geyser_pool_discovery: ignoring CPMM account (wrong size)"
            );
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
            active_id: None,
            bin_step: None,
            tick_current_index: None,
            tick_spacing: None,
        })
    }

    /// Parse Meteora DLMM pool account (LB Pair - 904 bytes)
    /// Source: Meteora DLMM program (LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo)
    fn parse_meteora_dlmm_pool(data: &[u8]) -> Option<PoolData> {
        // Meteora DLMM LB Pair account size: 904 bytes
        if data.len() != 904 {
            tracing::debug!(
                len = data.len(),
                "geyser_pool_discovery: ignoring Meteora DLMM account (wrong size)"
            );
            return None;
        }

        // Use existing parser from meteora_dlmm_layout
        let parsed = crate::solana::dex::meteora_dlmm_layout::DlmmPool::parse(data).ok()?;

        // Sanity check: mints should not be zero
        if parsed.token_x_mint.to_bytes() == [0u8; 32]
            || parsed.token_y_mint.to_bytes() == [0u8; 32]
        {
            tracing::debug!("geyser_pool_discovery: Meteora DLMM pool has zero mints");
            return None;
        }

        // IMPORTANT for Meteora DLMM:
        //
        // The lb_pair account has fixed positions for token_x/token_y and reserve_x/reserve_y.
        // The Meteora swap instruction requires these accounts in their ORIGINAL positions -
        // swapping them causes ConstraintHasOne errors.
        //
        // We send everything in the original lb_pair structure order:
        // - base_mint = token_x_mint (position in lb_pair, not semantically "base")
        // - quote_mint = token_y_mint
        // - coin_vault = reserve_x (token_x vault)
        // - pc_vault = reserve_y (token_y vault)
        //
        // The swap builder in execution-engine will determine swap direction based on
        // which token is actually being traded.
        let base_mint = parsed.token_x_mint;
        let quote_mint = parsed.token_y_mint;
        let coin_vault = parsed.reserve_x;
        let pc_vault = parsed.reserve_y;

        // Estimate liquidity from bin parameters
        // liquidity (u128) represents concentrated liquidity at active bin
        let liquidity_lamports = if parsed.active_id > 0 && parsed.bin_step > 0 {
            // Conservative estimate based on active bin liquidity
            // Real reserves will be fetched from vaults in background
            (parsed.active_id as u64)
                .saturating_mul(1_000_000)
                .min(100_000_000_000) // Cap at 100 SOL
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
            active_id: Some(parsed.active_id),
            bin_step: Some(parsed.bin_step),
            tick_current_index: None,
            tick_spacing: None,
        })
    }

    /// Parse Meteora CPMM pool account (DAMM V2 - 397 bytes)
    /// Source: Meteora CPMM program (cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D)
    ///
    /// This is simpler than DLMM - uses standard constant product x*y=k formula.
    fn parse_meteora_cpmm_pool(data: &[u8]) -> Option<PoolData> {
        use crate::solana::dex::meteora_cpmm_layout::{CpmmPool, CPMM_POOL_SIZE};

        // Meteora CPMM pool account size: 397 bytes
        if data.len() < CPMM_POOL_SIZE {
            tracing::debug!(
                len = data.len(),
                expected = CPMM_POOL_SIZE,
                "geyser_pool_discovery: ignoring Meteora CPMM account (wrong size)"
            );
            return None;
        }

        let parsed = CpmmPool::parse(data).ok()?;

        // Sanity check: mints should not be zero
        if parsed.token_0_mint.to_bytes() == [0u8; 32]
            || parsed.token_1_mint.to_bytes() == [0u8; 32]
        {
            tracing::debug!("geyser_pool_discovery: Meteora CPMM pool has zero mints");
            return None;
        }

        // Check pool is active (status < 2)
        if !parsed.is_active() {
            tracing::debug!(
                status = parsed.status,
                "geyser_pool_discovery: Meteora CPMM pool is not active"
            );
            return None;
        }

        tracing::info!(
            token_0 = %parsed.token_0_mint,
            token_1 = %parsed.token_1_mint,
            decimals_0 = parsed.mint_0_decimals,
            decimals_1 = parsed.mint_1_decimals,
            "geyser_pool_discovery: Meteora CPMM pool parsed"
        );

        // For CPMM, we use token_0 as base and token_1 as quote
        // The actual reserves will be fetched from vaults
        Some(PoolData {
            base_mint: parsed.token_0_mint,
            quote_mint: parsed.token_1_mint,
            base_decimals: parsed.mint_0_decimals,
            quote_decimals: parsed.mint_1_decimals,
            liquidity_lamports: 5_000_000_000, // 5 SOL default, will be updated from vaults
            coin_vault: Some(parsed.token_0_vault),
            pc_vault: Some(parsed.token_1_vault),
            active_id: None,
            bin_step: None,
            tick_current_index: None,
            tick_spacing: None,
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
            active_id: None,
            bin_step: None,
            // Orca-specific: tick_current_index and tick_spacing from parsed whirlpool
            tick_current_index: Some(parsed.tick_current_index),
            tick_spacing: Some(parsed.tick_spacing),
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
            active_id: None,
            bin_step: None,
            tick_current_index: None,
            tick_spacing: None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DexType {
    RaydiumAmmV4,
    RaydiumCpmm,
    OrcaWhirlpool,
    MeteoraDlmm,
    MeteoraCpmm,
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
            DexType::MeteoraCpmm => write!(f, "meteora_cpmm"),
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
    /// Meteora DLMM: active bin ID (current price bin)
    pub active_id: Option<i32>,
    /// Meteora DLMM: bin step (price step between bins in bps)
    pub bin_step: Option<u16>,
    /// Orca Whirlpool: tick_current_index (current price tick)
    pub tick_current_index: Option<i32>,
    /// Orca Whirlpool: tick_spacing
    pub tick_spacing: Option<u16>,
}

struct PoolData {
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_decimals: u8,
    quote_decimals: u8,
    liquidity_lamports: u64,
    coin_vault: Option<Pubkey>,
    pc_vault: Option<Pubkey>,
    /// Meteora DLMM: active bin ID (current price bin)
    active_id: Option<i32>,
    /// Meteora DLMM: bin step (price step between bins in bps)
    bin_step: Option<u16>,
    /// Orca Whirlpool: tick_current_index (current price tick)
    tick_current_index: Option<i32>,
    /// Orca Whirlpool: tick_spacing
    tick_spacing: Option<u16>,
}

#[cfg(test)]
mod pool_discovery_ingest_tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;
    use std::time::Instant;

    fn pumpfun_create_tx_update() -> GeyserTransactionUpdate {
        let pump = Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap();
        let mint = Pubkey::new_unique();
        let creator = Pubkey::new_unique();
        let mut ixs = vec![Pubkey::default(); 8];
        ixs[0] = mint;
        ixs[7] = creator;
        GeyserTransactionUpdate {
            signature: "testsig".to_string(),
            slot: 42,
            account_keys: vec![pump],
            instruction_accounts: ixs,
            instruction_data: vec![0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77, 0, 0, 0, 0],
            inner_instructions: vec![],
            pre_token_balances: vec![],
            post_token_balances: vec![],
            pre_balances: vec![],
            post_balances: vec![],
            fee_lamports: 0,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        }
    }

    #[tokio::test]
    async fn spawn_unified_tx_path_emits_pumpfun_pool_event() {
        let (acc_tx, acc_rx) = broadcast::channel(8);
        let pool_acc = acc_tx.subscribe();
        drop(acc_rx);

        let (tx_tx, tx_rx) = broadcast::channel(8);
        let pool_tx = tx_tx.subscribe();
        drop(tx_rx);

        let rpc = Arc::new(crate::solana::rpc::SolanaRpc::new("http://127.0.0.1:8899"));
        let mut out = PoolDiscoveryIngest::spawn_unified(pool_acc, pool_tx, Arc::clone(&rpc));

        let upd = pumpfun_create_tx_update();
        let expected_mint = upd.instruction_accounts[0];
        assert!(tx_tx.send(upd).is_ok());

        let ev = tokio::time::timeout(std::time::Duration::from_secs(2), out.recv())
            .await
            .expect("timed out waiting for pool discovery event")
            .expect("pool discovery channel closed");

        assert_eq!(ev.dex_type, DexType::PumpFun);
        assert_eq!(ev.base_mint, expected_mint);
    }
}
