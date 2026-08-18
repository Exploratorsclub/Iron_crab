//! Shared Pool Cache Sync — JetStream Bootstrap & Incremental Updates
//!
//! This module provides shared functions for building and maintaining a SLAVE
//! LivePoolCache from JetStream PoolCacheUpdate events. Used by both
//! execution-engine and momentum-bot.
//!
//! # Architecture
//!
//! ```text
//! market-data (MASTER LivePoolCache)
//!     │
//!     ├── publishes PoolCacheUpdate on JetStream
//!     │
//!     ├──→ execution-engine (SLAVE LivePoolCache)
//!     │
//!     └──→ momentum-bot    (SLAVE LivePoolCache)
//! ```

use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tracing::{debug, info, warn};

use crate::execution::live_pool_cache::{
    pool_state_has_reserve_basis, CachedPoolState, LivePoolCache, MeteoraCpmmState, MeteoraState,
    OrcaWhirlpoolState, PumpAmmState, PumpFunState, RaydiumAmmState, RaydiumCpmmState,
};
use crate::ipc::{
    PoolCacheUpdate, PoolCacheUpdateType, NATIVE_SOL_MINT,
    POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY, POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY,
    POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY, POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY,
    POOL_CACHE_UPDATE_METEORA_DLMM_ONCHAIN_MINTS_KEY, POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY,
    POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY, POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY,
    POOL_CACHE_UPDATE_ORCA_ONCHAIN_MINTS_KEY, POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY,
    POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY, POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY,
    POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY, POOL_CACHE_UPDATE_ORCA_TOKEN_A_PROGRAM_KEY,
    POOL_CACHE_UPDATE_ORCA_TOKEN_B_PROGRAM_KEY, POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY,
    POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY,
};
use crate::nats::{slave_consumer_config, NatsClient, STREAM_NAME};

/// Extract base_reserve and quote_reserve from CachedPoolState (for merge logic).
fn extract_reserves(state: &CachedPoolState) -> (u64, u64) {
    match state {
        CachedPoolState::Orca(s) => (
            s.vault_a_balance.unwrap_or(0),
            s.vault_b_balance.unwrap_or(0),
        ),
        CachedPoolState::RaydiumAmm(s) => (s.coin_reserve.unwrap_or(0), s.pc_reserve.unwrap_or(0)),
        CachedPoolState::RaydiumCpmm(s) => (s.reserve_0.unwrap_or(0), s.reserve_1.unwrap_or(0)),
        CachedPoolState::Meteora(s) => {
            let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
            let rx = s.reserve_x_balance.unwrap_or(0);
            let ry = s.reserve_y_balance.unwrap_or(0);
            if s.token_y_mint == sol {
                (rx, ry)
            } else if s.token_x_mint == sol {
                (ry, rx)
            } else {
                (rx, ry)
            }
        }
        CachedPoolState::PumpAmm(s) => (s.base_reserve.unwrap_or(0), s.quote_reserve.unwrap_or(0)),
        CachedPoolState::PumpFun(s) => (s.virtual_token_reserves, s.virtual_sol_reserves),
        CachedPoolState::MeteoraCpmm(s) => {
            // SLAVE may store reserves in on-chain token_0/token_1 order (when on-chain mint metadata
            // is present). BalanceUpdated merge treats the tuple as normalized (base, quote); map
            // like `meteora_cpmm_readiness_for_pool_cache_update` (non-SOL leg = base when SOL present).
            let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
            if s.token_1_mint == sol {
                (s.reserve_0, s.reserve_1)
            } else if s.token_0_mint == sol {
                (s.reserve_1, s.reserve_0)
            } else {
                (s.reserve_0, s.reserve_1)
            }
        }
    }
}

/// Build minimal CachedPoolState from PoolCacheUpdate (for JetStream bootstrap/sync)
///
/// Since PoolCacheUpdate only contains reserves (not full account data), we create
/// minimal state structures. Full account updates from Geyser will refresh these later.
///
/// Use `base_reserve` and `quote_reserve` when provided (for merge); otherwise uses update values.
pub fn build_minimal_pool_state(update: &PoolCacheUpdate) -> Option<(Pubkey, CachedPoolState)> {
    build_minimal_pool_state_with_reserves(update, update.base_reserve, update.quote_reserve)
}

/// Map normalized JetStream reserves onto on-chain Orca vault_a/vault_b balances.
fn orca_map_normalized_reserves_to_vault_balances(
    onchain_mint_a: Pubkey,
    onchain_mint_b: Pubkey,
    normalized_base: Pubkey,
    normalized_quote: Pubkey,
    base_reserve: u64,
    quote_reserve: u64,
) -> (Option<u64>, Option<u64>) {
    if onchain_mint_a == normalized_base && onchain_mint_b == normalized_quote {
        (Some(base_reserve), Some(quote_reserve))
    } else if onchain_mint_a == normalized_quote && onchain_mint_b == normalized_base {
        (Some(quote_reserve), Some(base_reserve))
    } else {
        (Some(base_reserve), Some(quote_reserve))
    }
}

/// Build minimal state with explicit reserves (used for BalanceUpdated merge).
fn build_minimal_pool_state_with_reserves(
    update: &PoolCacheUpdate,
    base_reserve: u64,
    quote_reserve: u64,
) -> Option<(Pubkey, CachedPoolState)> {
    let pool_addr = Pubkey::from_str(&update.pool_address).ok()?;
    let base_mint = Pubkey::from_str(&update.base_mint).ok()?;
    let quote_mint = Pubkey::from_str(&update.quote_mint).ok()?;

    // Build minimal state based on DEX type
    let state = match update.dex.as_str() {
        "orca" => {
            let meta = update.metadata.as_ref();
            let (token_vault_a, token_vault_b) = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY))
                .and_then(|s| {
                    let mut it = s.split(',').filter_map(|a| Pubkey::from_str(a.trim()).ok());
                    match (it.next(), it.next()) {
                        (Some(va), Some(vb)) => Some((va, vb)),
                        _ => None,
                    }
                })
                .unwrap_or((Pubkey::default(), Pubkey::default()));

            let tick_current_index = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY))
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);
            let tick_spacing = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY))
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(0);
            let sqrt_price = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY))
                .and_then(|s| s.parse::<u128>().ok())
                .unwrap_or(0);
            let liquidity = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY))
                .and_then(|s| s.parse::<u128>().ok())
                .unwrap_or(0);
            let fee_rate = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY))
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(0);
            let protocol_fee_rate = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY))
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(0);

            let token_a_program = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_TOKEN_A_PROGRAM_KEY))
                .and_then(|s| Pubkey::from_str(s.trim()).ok());
            let token_b_program = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_TOKEN_B_PROGRAM_KEY))
                .and_then(|s| Pubkey::from_str(s.trim()).ok());

            let (token_mint_a, token_mint_b, vault_a_balance, vault_b_balance) = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_ONCHAIN_MINTS_KEY))
                .and_then(|s| {
                    let mut it = s.split(',').filter_map(|a| Pubkey::from_str(a.trim()).ok());
                    match (it.next(), it.next()) {
                        (Some(ma), Some(mb)) => Some((ma, mb)),
                        _ => None,
                    }
                })
                .map(|(ma, mb)| {
                    let (va, vb) = orca_map_normalized_reserves_to_vault_balances(
                        ma,
                        mb,
                        base_mint,
                        quote_mint,
                        base_reserve,
                        quote_reserve,
                    );
                    (ma, mb, va, vb)
                })
                .unwrap_or((
                    base_mint,
                    quote_mint,
                    Some(base_reserve),
                    Some(quote_reserve),
                ));

            CachedPoolState::Orca(OrcaWhirlpoolState {
                token_mint_a,
                token_mint_b,
                token_vault_a,
                token_vault_b,
                tick_current_index,
                sqrt_price,
                liquidity,
                fee_rate,
                protocol_fee_rate,
                tick_spacing,
                vault_a_balance,
                vault_b_balance,
                token_a_program,
                token_b_program,
            })
        }
        "raydium" => {
            let serum_bids = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("serum_bids"))
                .and_then(|s| Pubkey::from_str(s).ok());
            let serum_asks = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("serum_asks"))
                .and_then(|s| Pubkey::from_str(s).ok());
            let serum_event_queue = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("serum_event_queue"))
                .and_then(|s| Pubkey::from_str(s).ok());
            let market_id = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("market_id"))
                .and_then(|s| Pubkey::from_str(s).ok())
                .unwrap_or_default();

            CachedPoolState::RaydiumAmm(RaydiumAmmState {
                base_mint,
                quote_mint,
                coin_vault: Pubkey::default(),
                pc_vault: Pubkey::default(),
                base_decimals: 0,
                quote_decimals: 0,
                coin_reserve: Some(base_reserve),
                pc_reserve: Some(quote_reserve),
                market_id,
                serum_bids,
                serum_asks,
                serum_event_queue,
            })
        }
        "raydium_cpmm" => CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: base_mint,
            token_1_mint: quote_mint,
            token_0_vault: Pubkey::default(),
            token_1_vault: Pubkey::default(),
            reserve_0: Some(base_reserve),
            reserve_1: Some(quote_reserve),
        }),
        "meteora_dlmm" => {
            let meta = update.metadata.as_ref();
            let (reserve_x, reserve_y) = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY))
                .and_then(|s| {
                    let mut it = s.split(',').filter_map(|a| Pubkey::from_str(a.trim()).ok());
                    match (it.next(), it.next()) {
                        (Some(vx), Some(vy)) => Some((vx, vy)),
                        _ => None,
                    }
                })
                .unwrap_or((Pubkey::default(), Pubkey::default()));

            let active_id = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY))
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);
            let bin_step = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY))
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(0);

            let (token_x_mint, token_y_mint, reserve_x_balance, reserve_y_balance) = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_DLMM_ONCHAIN_MINTS_KEY))
                .and_then(|s| {
                    let mut it = s.split(',').filter_map(|a| Pubkey::from_str(a.trim()).ok());
                    match (it.next(), it.next()) {
                        (Some(tx), Some(ty)) => Some((tx, ty)),
                        _ => None,
                    }
                })
                .map(|(tx, ty)| {
                    let (rx, ry) = if tx == base_mint && ty == quote_mint {
                        (base_reserve, quote_reserve)
                    } else if tx == quote_mint && ty == base_mint {
                        (quote_reserve, base_reserve)
                    } else {
                        (base_reserve, quote_reserve)
                    };
                    (tx, ty, rx, ry)
                })
                .unwrap_or((base_mint, quote_mint, base_reserve, quote_reserve));

            CachedPoolState::Meteora(MeteoraState {
                token_x_mint,
                token_y_mint,
                reserve_x,
                reserve_y,
                active_id,
                bin_step,
                reserve_x_balance: Some(reserve_x_balance),
                reserve_y_balance: Some(reserve_y_balance),
            })
        }
        "meteora_cpmm" => {
            // `base_mint` / `quote_mint` / reserves are normalized (non-SOL first). On-chain
            // Meteora pool state uses token_0 / token_1 — may be SOL,base. Optional metadata carries
            // true on-chain mint order so SLAVE bootstrap from BalanceUpdated alone stays correct.
            let meta = update.metadata.as_ref();
            let (token_0_mint, token_1_mint, reserve_0, reserve_1) = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY))
                .and_then(|s| {
                    let mut it = s.split(',').filter_map(|a| Pubkey::from_str(a.trim()).ok());
                    match (it.next(), it.next()) {
                        (Some(t0), Some(t1)) => Some((t0, t1)),
                        _ => None,
                    }
                })
                .map(|(t0, t1)| {
                    let (r0, r1) = if t0 == base_mint && t1 == quote_mint {
                        (base_reserve, quote_reserve)
                    } else if t0 == quote_mint && t1 == base_mint {
                        (quote_reserve, base_reserve)
                    } else {
                        (base_reserve, quote_reserve)
                    };
                    (t0, t1, r0, r1)
                })
                .unwrap_or((base_mint, quote_mint, base_reserve, quote_reserve));

            // Only map vaults here when on-chain mint order is known. Otherwise leave defaults so
            // `BalanceUpdated` merge can use existing Geyser row + normalized vault metadata.
            let onchain_meta = meta
                .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY))
                .is_some();
            let (token_0_vault, token_1_vault) = if onchain_meta {
                meta.and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY))
                    .and_then(|s| {
                        let mut it = s.split(',').filter_map(|a| Pubkey::from_str(a.trim()).ok());
                        match (it.next(), it.next()) {
                            (Some(bv), Some(qv)) => Some((bv, qv)),
                            _ => None,
                        }
                    })
                    .map(|(bv, qv)| {
                        let v0 = if token_0_mint == base_mint {
                            bv
                        } else if token_0_mint == quote_mint {
                            qv
                        } else {
                            Pubkey::default()
                        };
                        let v1 = if token_1_mint == base_mint {
                            bv
                        } else if token_1_mint == quote_mint {
                            qv
                        } else {
                            Pubkey::default()
                        };
                        (v0, v1)
                    })
                    .unwrap_or((Pubkey::default(), Pubkey::default()))
            } else {
                (Pubkey::default(), Pubkey::default())
            };

            CachedPoolState::MeteoraCpmm(MeteoraCpmmState {
                token_0_mint,
                token_1_mint,
                token_0_vault,
                token_1_vault,
                amm_config: Pubkey::default(),
                observation_key: Pubkey::default(),
                token_0_program: Pubkey::default(),
                token_1_program: Pubkey::default(),
                reserve_0,
                reserve_1,
                mint_0_decimals: 0,
                mint_1_decimals: 0,
                status: 0,
            })
        }
        "pump_amm" => {
            let creator = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("creator"))
                .and_then(|s| Pubkey::from_str(s).ok());

            let pool_accounts: Vec<Pubkey> = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("pool_accounts"))
                .map(|s| {
                    s.split(',')
                        .filter_map(|a| Pubkey::from_str(a).ok())
                        .collect()
                })
                .unwrap_or_default();

            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint,
                pool_base_token_account: Pubkey::default(),
                pool_quote_token_account: Pubkey::default(),
                base_reserve: Some(base_reserve),
                quote_reserve: Some(quote_reserve),
                pool_accounts,
                creator,
            })
        }
        "pumpfun" => {
            let creator = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("creator"))
                .and_then(|s| Pubkey::from_str(s).ok())
                .unwrap_or_default();

            let associated_bonding_curve = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("associated_bonding_curve"))
                .and_then(|s| Pubkey::from_str(s).ok())
                .unwrap_or_default();

            let complete = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("complete"))
                .map(|s| s == "true")
                .unwrap_or(false);

            let real_token_reserves = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("real_token_reserves"))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let real_sol_reserves = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("real_sol_reserves"))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            if creator != Pubkey::default() {
                debug!(
                    pool = %pool_addr,
                    creator = %creator,
                    real_token_reserves,
                    real_sol_reserves,
                    "SLAVE CACHE: PumpFun state from PoolCacheUpdate metadata"
                );
            }

            CachedPoolState::PumpFun(PumpFunState {
                token_mint: base_mint,
                bonding_curve: pool_addr,
                associated_bonding_curve,
                virtual_sol_reserves: quote_reserve,
                virtual_token_reserves: base_reserve,
                real_sol_reserves,
                real_token_reserves,
                complete,
                creator,
                cashback_enabled: update
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("cashback_enabled"))
                    .map(|v| v == "true")
                    .unwrap_or(false), // Resolved from JetStream metadata (propagated by market-data from Geyser)
            })
        }
        _ => {
            debug!(dex = %update.dex, "Unsupported DEX type for minimal state");
            return None;
        }
    };

    Some((pool_addr, state))
}

/// P3 #13: Apply base_decimals and quote_decimals from PoolCacheUpdate metadata to LivePoolCache.
fn apply_decimals_from_metadata(cache: &LivePoolCache, update: &PoolCacheUpdate) {
    let meta = match update.metadata.as_ref() {
        Some(m) if !m.is_empty() => m,
        _ => return,
    };
    if let (Ok(base_mint), Some(d)) = (
        Pubkey::from_str(&update.base_mint),
        meta.get("base_decimals").and_then(|s| s.parse::<u8>().ok()),
    ) {
        cache.set_mint_decimals(base_mint, d);
    }
    if let (Ok(quote_mint), Some(d)) = (
        Pubkey::from_str(&update.quote_mint),
        meta.get("quote_decimals")
            .and_then(|s| s.parse::<u8>().ok()),
    ) {
        cache.set_mint_decimals(quote_mint, d);
    }
}

/// Outcome of applying a `PoolCacheUpdate` or Core `PoolStateUpdate` bridge to SLAVE cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolCacheApplyOutcome {
    Applied,
    /// `dex == "restored"` vault snapshot rows — not a quotable pool cache update.
    SkippedRestoredDex,
    /// No-op paths (`PoolRemoved`, etc.) — not a reject.
    SkippedNoOp,
    RejectedStaleSlot,
    RejectedBuildNone,
    RejectedUnsupportedDex,
    RejectedInvalidPoolAddress,
}

impl PoolCacheApplyOutcome {
    pub fn reject_reason_label(self) -> Option<&'static str> {
        match self {
            Self::Applied | Self::SkippedRestoredDex | Self::SkippedNoOp => None,
            Self::RejectedStaleSlot => Some("stale_slot"),
            Self::RejectedBuildNone => Some("build_none"),
            Self::RejectedUnsupportedDex => Some("unsupported_dex"),
            Self::RejectedInvalidPoolAddress => Some("invalid_pool_address"),
        }
    }

    pub fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }

    pub fn metrics_reason_label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::SkippedRestoredDex => "skipped_restored_dex",
            Self::SkippedNoOp => "skipped_noop",
            Self::RejectedStaleSlot => "stale_slot",
            Self::RejectedBuildNone => "build_none",
            Self::RejectedUnsupportedDex => "unsupported_dex",
            Self::RejectedInvalidPoolAddress => "invalid_pool_address",
        }
    }
}

/// Apply a single PoolCacheUpdate to a LivePoolCache.
///
/// For BalanceUpdated: merges partial updates (one vault at a time) with existing cache state.
/// If the update has only base or only quote (the other is 0), we preserve the existing value
/// for the other reserve to avoid "out=0" quote failures (e.g. meteora: missing reserves).
///
/// Returns true if the cache was modified.
pub fn apply_pool_cache_update(cache: &LivePoolCache, update: &PoolCacheUpdate) -> bool {
    apply_pool_cache_update_outcome(cache, update).is_applied()
}

/// Detailed apply outcome for observability (reason + dex metrics).
pub fn apply_pool_cache_update_outcome(
    cache: &LivePoolCache,
    update: &PoolCacheUpdate,
) -> PoolCacheApplyOutcome {
    if update.dex == "restored" {
        return PoolCacheApplyOutcome::SkippedRestoredDex;
    }
    apply_pool_cache_update_outcome_inner(cache, update)
}

fn apply_pool_cache_update_outcome_inner(
    cache: &LivePoolCache,
    update: &PoolCacheUpdate,
) -> PoolCacheApplyOutcome {
    match update.update_type {
        PoolCacheUpdateType::PoolDiscovered => {
            if let Some((pool_addr, mut minimal_state)) = build_minimal_pool_state(update) {
                if update.dex == "pump_amm" {
                    if let Some(existing) = cache.get(&pool_addr) {
                        if let (
                            CachedPoolState::PumpAmm(ref existing_pump),
                            CachedPoolState::PumpAmm(ref mut new_pump),
                        ) = (&existing, &mut minimal_state)
                        {
                            if new_pump.pool_accounts.is_empty()
                                && !existing_pump.pool_accounts.is_empty()
                            {
                                new_pump.pool_accounts = existing_pump.pool_accounts.clone();
                            }
                            if new_pump.creator.is_none() && existing_pump.creator.is_some() {
                                new_pump.creator = existing_pump.creator;
                            }
                            // Bug #27 / #28: PoolDiscovered with 0/0 or partial reserves must not
                            // wipe good SLAVE reserves (e.g. FIX-33 trade publish without vault scalars).
                            let (eb, eq) = (
                                existing_pump.base_reserve.unwrap_or(0),
                                existing_pump.quote_reserve.unwrap_or(0),
                            );
                            let (nb, nq) = (
                                new_pump.base_reserve.unwrap_or(0),
                                new_pump.quote_reserve.unwrap_or(0),
                            );
                            let merged_base = if nb > 0 { nb } else { eb };
                            let merged_quote = if nq > 0 { nq } else { eq };
                            new_pump.base_reserve = Some(merged_base);
                            new_pump.quote_reserve = Some(merged_quote);
                        }
                    }
                }
                if update.dex == "raydium_cpmm" {
                    if let CachedPoolState::RaydiumCpmm(ref mut new_cpmm) = minimal_state {
                        if let Some(m) = update.metadata.as_ref() {
                            if let Some(s) = m.get(POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY) {
                                let mut it =
                                    s.split(',').filter_map(|a| Pubkey::from_str(a.trim()).ok());
                                if let (Some(v0), Some(v1)) = (it.next(), it.next()) {
                                    new_cpmm.token_0_vault = v0;
                                    new_cpmm.token_1_vault = v1;
                                }
                            }
                        }
                    }
                }
                // PoolDiscovered without DLMM metadata (legacy JetStream): keep structural fields from SLAVE.
                if update.dex == "meteora_dlmm" {
                    let um = update.metadata.as_ref();
                    if let CachedPoolState::Meteora(ref mut new_m) = minimal_state {
                        if let Some(CachedPoolState::Meteora(ex)) = cache.get(&pool_addr).as_ref() {
                            if new_m.reserve_x == Pubkey::default()
                                && ex.reserve_x != Pubkey::default()
                            {
                                new_m.reserve_x = ex.reserve_x;
                            }
                            if new_m.reserve_y == Pubkey::default()
                                && ex.reserve_y != Pubkey::default()
                            {
                                new_m.reserve_y = ex.reserve_y;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY))
                                .is_none()
                            {
                                new_m.active_id = ex.active_id;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY))
                                .is_none()
                            {
                                new_m.bin_step = ex.bin_step;
                            }
                        }
                    }
                }
                if !cache.upsert(pool_addr, minimal_state, update.geyser_slot) {
                    if update.geyser_slot > 0 {
                        crate::metrics::inc_pool_cache_apply_rejected_stale_slot_total();
                    }
                    return PoolCacheApplyOutcome::RejectedStaleSlot;
                }
                if update.dex == "pump_amm" {
                    cache.merge_pump_amm_sell_layout_from_metadata(
                        &pool_addr,
                        update.metadata.as_ref(),
                    );
                    if update
                        .metadata
                        .as_ref()
                        .is_some_and(|m| m.contains_key("pump_amm_sell_layout_ready"))
                    {
                        cache.set_pump_amm_pool_accounts_readiness_authoritative(
                            pool_addr,
                            update.effective_dex_readiness(),
                        );
                    } else {
                        cache.merge_pump_amm_pool_accounts_readiness(
                            pool_addr,
                            update.effective_dex_readiness(),
                        );
                    }
                }
                if update.dex == "raydium_cpmm" {
                    cache.merge_raydium_cpmm_pool_readiness(
                        pool_addr,
                        update.effective_dex_readiness(),
                    );
                }
                if update.dex == "raydium" {
                    cache.merge_raydium_amm_pool_readiness(
                        pool_addr,
                        update.effective_dex_readiness(),
                    );
                }
                if update.dex == "pumpfun" {
                    cache.merge_pumpfun_bonding_readiness(
                        pool_addr,
                        update.effective_dex_readiness(),
                    );
                }
                if update.dex == "meteora_cpmm" {
                    cache.merge_meteora_cpmm_pool_readiness(
                        pool_addr,
                        update.effective_dex_readiness(),
                    );
                }
                if update.dex == "orca" {
                    cache.merge_orca_pool_readiness(pool_addr, update.effective_dex_readiness());
                }
                if update.dex == "meteora_dlmm" {
                    cache.merge_meteora_dlmm_pool_readiness(
                        pool_addr,
                        update.effective_dex_readiness(),
                    );
                    cache.merge_meteora_dlmm_bitmap_extension_from_metadata(
                        &pool_addr,
                        update.metadata.as_ref(),
                    );
                }
                // P3 #13: Propagate base_decimals and quote_decimals to SLAVE cache
                apply_decimals_from_metadata(cache, update);
                return PoolCacheApplyOutcome::Applied;
            }
            return PoolCacheApplyOutcome::RejectedBuildNone;
        }
        PoolCacheUpdateType::BalanceUpdated => {
            let pool_addr = match Pubkey::from_str(&update.pool_address) {
                Ok(p) => p,
                Err(_) => return PoolCacheApplyOutcome::RejectedInvalidPoolAddress,
            };
            let existing = cache.get(&pool_addr);
            let (base_reserve, quote_reserve) = if let Some(ref ex) = existing {
                let (eb, eq) = extract_reserves(ex);
                let base = if update.base_reserve > 0 {
                    update.base_reserve
                } else {
                    eb
                };
                let quote = if update.quote_reserve > 0 {
                    update.quote_reserve
                } else {
                    eq
                };
                (base, quote)
            } else {
                (update.base_reserve, update.quote_reserve)
            };
            if let Some((addr, mut minimal_state)) =
                build_minimal_pool_state_with_reserves(update, base_reserve, quote_reserve)
            {
                // P3 #12: Preserve pool_accounts and creator for pump_amm when BalanceUpdated
                // has no metadata (BalanceUpdated never includes metadata). Otherwise we'd
                // overwrite good data from PoolDiscovered and force RPC fallback in Liquidation.
                if update.dex == "pump_amm" {
                    if let (
                        Some(CachedPoolState::PumpAmm(ref existing_pump)),
                        CachedPoolState::PumpAmm(ref mut new_pump),
                    ) = (existing.as_ref(), &mut minimal_state)
                    {
                        if new_pump.pool_accounts.is_empty()
                            && !existing_pump.pool_accounts.is_empty()
                        {
                            new_pump.pool_accounts = existing_pump.pool_accounts.clone();
                        }
                        if new_pump.creator.is_none() && existing_pump.creator.is_some() {
                            new_pump.creator = existing_pump.creator;
                        }
                    }
                }
                // Bug #25: Preserve cashback_enabled for pumpfun when BalanceUpdated has no metadata
                // (BalanceUpdated never includes metadata). Otherwise we'd overwrite true with false.
                if update.dex == "pumpfun" {
                    let metadata_has_cashback = update
                        .metadata
                        .as_ref()
                        .and_then(|m| m.get("cashback_enabled"))
                        .is_some();
                    if !metadata_has_cashback {
                        if let (
                            Some(CachedPoolState::PumpFun(ref existing_pump)),
                            CachedPoolState::PumpFun(ref mut new_pump),
                        ) = (existing.as_ref(), &mut minimal_state)
                        {
                            new_pump.cashback_enabled = existing_pump.cashback_enabled;
                        }
                    }
                }
                // BalanceUpdated has no vault pubkeys in the scalar fields — preserve from existing
                // or from metadata (`raydium_cpmm_vaults`: base_vault,quote_vault matching normalized
                // base_mint/quote_mint on the update) so reserve updates stay coherent.
                if update.dex == "raydium_cpmm" {
                    if let CachedPoolState::RaydiumCpmm(ref mut new_cpmm) = minimal_state {
                        // Prefer existing vaults only when both are non-default. Legacy JetStream
                        // rows without `raydium_cpmm_vaults` can leave default vaults in cache; a
                        // newer BalanceUpdated with metadata must still win (restart / upgrade).
                        let from_existing = existing.as_ref().and_then(|ex| {
                            if let CachedPoolState::RaydiumCpmm(s) = ex {
                                if s.token_0_vault != Pubkey::default()
                                    && s.token_1_vault != Pubkey::default()
                                {
                                    Some((s.token_0_vault, s.token_1_vault))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        });
                        let from_meta = update.metadata.as_ref().and_then(|m| {
                            m.get(POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY)
                                .and_then(|s| {
                                    let mut it = s
                                        .split(',')
                                        .filter_map(|a| Pubkey::from_str(a.trim()).ok());
                                    match (it.next(), it.next()) {
                                        (Some(v0), Some(v1)) => Some((v0, v1)),
                                        _ => None,
                                    }
                                })
                        });
                        if let Some((v0, v1)) = from_existing.or(from_meta) {
                            if new_cpmm.token_0_vault == Pubkey::default() {
                                new_cpmm.token_0_vault = v0;
                            }
                            if new_cpmm.token_1_vault == Pubkey::default() {
                                new_cpmm.token_1_vault = v1;
                            }
                        }
                    }
                }
                // BalanceUpdated: vault pubkeys only in metadata or existing Geyser state; preserve
                // programs/config/decimals/status from existing Meteora CPMM (minimal JetStream row).
                if update.dex == "meteora_cpmm" {
                    if let CachedPoolState::MeteoraCpmm(ref mut new_cpmm) = minimal_state {
                        // Without `meteora_cpmm_onchain_mints`, scalar mints are normalized base/quote only.
                        // Preserve on-chain token_0/token_1 from an existing SLAVE row so vault metadata
                        // remap stays consistent (SOL-as-token_0 + vault-only metadata).
                        let update_has_onchain_mints = update
                            .metadata
                            .as_ref()
                            .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY))
                            .is_some();
                        if !update_has_onchain_mints {
                            if let Some(CachedPoolState::MeteoraCpmm(ex)) = existing.as_ref() {
                                new_cpmm.token_0_mint = ex.token_0_mint;
                                new_cpmm.token_1_mint = ex.token_1_mint;
                                if let (Ok(up_base), Ok(up_quote)) = (
                                    Pubkey::from_str(&update.base_mint),
                                    Pubkey::from_str(&update.quote_mint),
                                ) {
                                    new_cpmm.reserve_0 = if ex.token_0_mint == up_base {
                                        base_reserve
                                    } else if ex.token_0_mint == up_quote {
                                        quote_reserve
                                    } else {
                                        new_cpmm.reserve_0
                                    };
                                    new_cpmm.reserve_1 = if ex.token_1_mint == up_base {
                                        base_reserve
                                    } else if ex.token_1_mint == up_quote {
                                        quote_reserve
                                    } else {
                                        new_cpmm.reserve_1
                                    };
                                }
                            }
                        }

                        let from_existing_vaults = existing.as_ref().and_then(|ex| {
                            if let CachedPoolState::MeteoraCpmm(s) = ex {
                                if s.token_0_vault != Pubkey::default()
                                    && s.token_1_vault != Pubkey::default()
                                {
                                    Some((s.token_0_vault, s.token_1_vault))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        });
                        let from_meta_vaults = update.metadata.as_ref().and_then(|m| {
                            m.get(POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY)
                                .and_then(|s| {
                                    let mut it = s
                                        .split(',')
                                        .filter_map(|a| Pubkey::from_str(a.trim()).ok());
                                    match (it.next(), it.next()) {
                                        (Some(v0), Some(v1)) => Some((v0, v1)),
                                        _ => None,
                                    }
                                })
                        });
                        // Existing Geyser/JetStream row: vault pair is already token_0_vault,token_1_vault
                        // (on-chain order). Do not run base/quote remap on it — that double-swaps when
                        // SOL is token_0 but PoolCacheUpdate uses normalized mints.
                        if let Some((t0v, t1v)) = from_existing_vaults {
                            if new_cpmm.token_0_vault == Pubkey::default() {
                                new_cpmm.token_0_vault = t0v;
                            }
                            if new_cpmm.token_1_vault == Pubkey::default() {
                                new_cpmm.token_1_vault = t1v;
                            }
                        } else if let Some((base_vault, quote_vault)) = from_meta_vaults {
                            // Metadata: `base_vault,quote_vault` matches normalized update.base_mint /
                            // update.quote_mint. Map onto token_0/token_1 using authoritative on-chain
                            // mint order when SLAVE already has it; else minimal state's mints.
                            let (auth_t0_mint, auth_t1_mint) =
                                if let Some(CachedPoolState::MeteoraCpmm(ex)) = existing.as_ref() {
                                    (ex.token_0_mint, ex.token_1_mint)
                                } else {
                                    (new_cpmm.token_0_mint, new_cpmm.token_1_mint)
                                };
                            if let (Ok(up_base), Ok(up_quote)) = (
                                Pubkey::from_str(&update.base_mint),
                                Pubkey::from_str(&update.quote_mint),
                            ) {
                                let t0v = if auth_t0_mint == up_base {
                                    base_vault
                                } else if auth_t0_mint == up_quote {
                                    quote_vault
                                } else {
                                    Pubkey::default()
                                };
                                let t1v = if auth_t1_mint == up_base {
                                    base_vault
                                } else if auth_t1_mint == up_quote {
                                    quote_vault
                                } else {
                                    Pubkey::default()
                                };
                                if new_cpmm.token_0_vault == Pubkey::default() {
                                    new_cpmm.token_0_vault = t0v;
                                }
                                if new_cpmm.token_1_vault == Pubkey::default() {
                                    new_cpmm.token_1_vault = t1v;
                                }
                            }
                        }
                        if let Some(CachedPoolState::MeteoraCpmm(ex)) = existing.as_ref() {
                            new_cpmm.mint_0_decimals = ex.mint_0_decimals;
                            new_cpmm.mint_1_decimals = ex.mint_1_decimals;
                            new_cpmm.status = ex.status;
                            if new_cpmm.token_0_vault == Pubkey::default()
                                && ex.token_0_vault != Pubkey::default()
                            {
                                new_cpmm.token_0_vault = ex.token_0_vault;
                            }
                            if new_cpmm.token_1_vault == Pubkey::default()
                                && ex.token_1_vault != Pubkey::default()
                            {
                                new_cpmm.token_1_vault = ex.token_1_vault;
                            }
                            if new_cpmm.amm_config == Pubkey::default()
                                && ex.amm_config != Pubkey::default()
                            {
                                new_cpmm.amm_config = ex.amm_config;
                            }
                            if new_cpmm.observation_key == Pubkey::default()
                                && ex.observation_key != Pubkey::default()
                            {
                                new_cpmm.observation_key = ex.observation_key;
                            }
                            if new_cpmm.token_0_program == Pubkey::default()
                                && ex.token_0_program != Pubkey::default()
                            {
                                new_cpmm.token_0_program = ex.token_0_program;
                            }
                            if new_cpmm.token_1_program == Pubkey::default()
                                && ex.token_1_program != Pubkey::default()
                            {
                                new_cpmm.token_1_program = ex.token_1_program;
                            }
                        }
                    }
                }
                // BalanceUpdated often omits metadata: preserve Serum static accounts + vault pubkeys
                // from existing Raydium AMM state so SLAVE readiness does not spuriously downgrade.
                if update.dex == "raydium" {
                    if let CachedPoolState::RaydiumAmm(ref mut new_am) = minimal_state {
                        if let Some(CachedPoolState::RaydiumAmm(ex)) = existing.as_ref() {
                            if new_am.market_id == Pubkey::default()
                                && ex.market_id != Pubkey::default()
                            {
                                new_am.market_id = ex.market_id;
                            }
                            if new_am.serum_bids.is_none() {
                                new_am.serum_bids = ex.serum_bids;
                            }
                            if new_am.serum_asks.is_none() {
                                new_am.serum_asks = ex.serum_asks;
                            }
                            if new_am.serum_event_queue.is_none() {
                                new_am.serum_event_queue = ex.serum_event_queue;
                            }
                            if new_am.coin_vault == Pubkey::default() {
                                new_am.coin_vault = ex.coin_vault;
                            }
                            if new_am.pc_vault == Pubkey::default() {
                                new_am.pc_vault = ex.pc_vault;
                            }
                        }
                    }
                }
                // BalanceUpdated from vault path may omit whirlpool static metadata; preserve from SLAVE row
                // when this message does not carry the corresponding keys (tick 0 is valid — do not use `== 0` heuristics).
                if update.dex == "orca" {
                    let um = update.metadata.as_ref();
                    if let CachedPoolState::Orca(ref mut new_o) = minimal_state {
                        if let Some(CachedPoolState::Orca(ex)) = existing.as_ref() {
                            new_o.token_mint_a = ex.token_mint_a;
                            new_o.token_mint_b = ex.token_mint_b;
                            if let (Ok(up_base), Ok(up_quote)) = (
                                Pubkey::from_str(&update.base_mint),
                                Pubkey::from_str(&update.quote_mint),
                            ) {
                                let (va, vb) = orca_map_normalized_reserves_to_vault_balances(
                                    ex.token_mint_a,
                                    ex.token_mint_b,
                                    up_base,
                                    up_quote,
                                    base_reserve,
                                    quote_reserve,
                                );
                                new_o.vault_a_balance = va;
                                new_o.vault_b_balance = vb;
                            }
                            if new_o.token_vault_a == Pubkey::default()
                                && ex.token_vault_a != Pubkey::default()
                            {
                                new_o.token_vault_a = ex.token_vault_a;
                            }
                            if new_o.token_vault_b == Pubkey::default()
                                && ex.token_vault_b != Pubkey::default()
                            {
                                new_o.token_vault_b = ex.token_vault_b;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY))
                                .is_none()
                            {
                                new_o.tick_current_index = ex.tick_current_index;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY))
                                .is_none()
                            {
                                new_o.tick_spacing = ex.tick_spacing;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY))
                                .is_none()
                            {
                                new_o.sqrt_price = ex.sqrt_price;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY))
                                .is_none()
                            {
                                new_o.liquidity = ex.liquidity;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY))
                                .is_none()
                            {
                                new_o.fee_rate = ex.fee_rate;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY))
                                .is_none()
                            {
                                new_o.protocol_fee_rate = ex.protocol_fee_rate;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_TOKEN_A_PROGRAM_KEY))
                                .is_none()
                            {
                                new_o.token_a_program = ex.token_a_program;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_ORCA_TOKEN_B_PROGRAM_KEY))
                                .is_none()
                            {
                                new_o.token_b_program = ex.token_b_program;
                            }
                        }
                    }
                }
                // BalanceUpdated may omit DLMM static fields; `active_id == 0` / `bin_step == 0` are valid on-chain.
                if update.dex == "meteora_dlmm" {
                    let um = update.metadata.as_ref();
                    if let CachedPoolState::Meteora(ref mut new_m) = minimal_state {
                        let update_has_onchain_mints = um
                            .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_DLMM_ONCHAIN_MINTS_KEY))
                            .is_some();
                        if !update_has_onchain_mints {
                            if let Some(CachedPoolState::Meteora(ex)) = existing.as_ref() {
                                new_m.token_x_mint = ex.token_x_mint;
                                new_m.token_y_mint = ex.token_y_mint;
                                if let (Ok(up_base), Ok(up_quote)) = (
                                    Pubkey::from_str(&update.base_mint),
                                    Pubkey::from_str(&update.quote_mint),
                                ) {
                                    new_m.reserve_x_balance = if ex.token_x_mint == up_base {
                                        Some(base_reserve)
                                    } else if ex.token_x_mint == up_quote {
                                        Some(quote_reserve)
                                    } else {
                                        new_m.reserve_x_balance
                                    };
                                    new_m.reserve_y_balance = if ex.token_y_mint == up_base {
                                        Some(base_reserve)
                                    } else if ex.token_y_mint == up_quote {
                                        Some(quote_reserve)
                                    } else {
                                        new_m.reserve_y_balance
                                    };
                                }
                            }
                        }
                        if let Some(CachedPoolState::Meteora(ex)) = existing.as_ref() {
                            if new_m.reserve_x == Pubkey::default()
                                && ex.reserve_x != Pubkey::default()
                            {
                                new_m.reserve_x = ex.reserve_x;
                            }
                            if new_m.reserve_y == Pubkey::default()
                                && ex.reserve_y != Pubkey::default()
                            {
                                new_m.reserve_y = ex.reserve_y;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY))
                                .is_none()
                            {
                                new_m.active_id = ex.active_id;
                            }
                            if um
                                .and_then(|m| m.get(POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY))
                                .is_none()
                            {
                                new_m.bin_step = ex.bin_step;
                            }
                        }
                    }
                }
                let incoming_has_reserve_basis = pool_state_has_reserve_basis(&minimal_state);
                let upserted = cache.upsert(addr, minimal_state, update.geyser_slot);
                if !upserted {
                    if update.geyser_slot > 0 {
                        crate::metrics::inc_pool_cache_apply_rejected_stale_slot_total();
                        let age_sustained = incoming_has_reserve_basis
                            && cache.touch_freshness_on_existing_reserve_basis(&addr);
                        if age_sustained {
                            crate::metrics::inc_pool_cache_touch_freshness_on_stale_slot_total();
                        }
                        if !age_sustained {
                            return PoolCacheApplyOutcome::RejectedStaleSlot;
                        }
                    } else {
                        return PoolCacheApplyOutcome::RejectedStaleSlot;
                    }
                }
                if update.dex == "pump_amm" {
                    cache.merge_pump_amm_sell_layout_from_metadata(&addr, update.metadata.as_ref());
                    if update
                        .metadata
                        .as_ref()
                        .is_some_and(|m| m.contains_key("pump_amm_sell_layout_ready"))
                    {
                        cache.set_pump_amm_pool_accounts_readiness_authoritative(
                            addr,
                            update.effective_dex_readiness(),
                        );
                    } else {
                        cache.merge_pump_amm_pool_accounts_readiness(
                            addr,
                            update.effective_dex_readiness(),
                        );
                    }
                }
                if update.dex == "raydium_cpmm" {
                    cache.merge_raydium_cpmm_pool_readiness(addr, update.effective_dex_readiness());
                }
                if update.dex == "raydium" {
                    cache.merge_raydium_amm_pool_readiness(addr, update.effective_dex_readiness());
                }
                if update.dex == "pumpfun" {
                    cache.merge_pumpfun_bonding_readiness(addr, update.effective_dex_readiness());
                }
                if update.dex == "meteora_cpmm" {
                    cache.merge_meteora_cpmm_pool_readiness(addr, update.effective_dex_readiness());
                }
                if update.dex == "orca" {
                    cache.merge_orca_pool_readiness(addr, update.effective_dex_readiness());
                }
                if update.dex == "meteora_dlmm" {
                    cache.merge_meteora_dlmm_pool_readiness(addr, update.effective_dex_readiness());
                    cache.merge_meteora_dlmm_bitmap_extension_from_metadata(
                        &addr,
                        update.metadata.as_ref(),
                    );
                }
                // P3 #13: Apply decimals from metadata when present (e.g. BalanceUpdated with metadata)
                apply_decimals_from_metadata(cache, update);
                return PoolCacheApplyOutcome::Applied;
            }
            return PoolCacheApplyOutcome::RejectedBuildNone;
        }
        PoolCacheUpdateType::PoolRemoved => {
            // Skip removed pools
        }
    }
    PoolCacheApplyOutcome::SkippedNoOp
}

/// Fields from Core NATS `MarketEventKind::PoolStateUpdate` for SLAVE cache bridge.
#[derive(Debug, Clone)]
pub struct PoolStateUpdateBridge<'a> {
    pub pool_address: &'a str,
    pub dex: &'a str,
    pub reserve_base: u64,
    pub reserve_quote: u64,
    pub base_mint: &'a str,
    pub quote_mint: &'a str,
    pub update_slot: u64,
}

/// Bridge Core NATS `PoolStateUpdate` (reserves + `update_slot`) into SLAVE `LivePoolCache`.
///
/// JetStream `PoolCacheUpdate` remains SSOT; this path is a low-latency supplement when MD
/// already published reserves on Core (e.g. open-position PumpFun pin after BondingCurve upsert).
pub fn apply_pool_state_update_to_cache(
    cache: &LivePoolCache,
    bridge: &PoolStateUpdateBridge<'_>,
) -> PoolCacheApplyOutcome {
    if bridge.dex == "restored" {
        return PoolCacheApplyOutcome::SkippedRestoredDex;
    }
    if bridge.reserve_base == 0 && bridge.reserve_quote == 0 {
        return PoolCacheApplyOutcome::SkippedNoOp;
    }
    let update = PoolCacheUpdate::new_balance_updated(
        "market-data",
        "",
        "",
        bridge.pool_address.to_string(),
        bridge.dex.to_string(),
        bridge.base_mint.to_string(),
        bridge.quote_mint.to_string(),
        bridge.reserve_base,
        bridge.reserve_quote,
        bridge.update_slot,
    );
    apply_pool_cache_update_outcome(cache, &update)
}

/// Bootstrap LivePoolCache from JetStream (state recovery after restart)
///
/// Pulls the last PoolCacheUpdate for each pool from JetStream, giving
/// immediate state recovery. After bootstrap, the caller should reuse
/// the returned consumer for incremental updates.
///
/// # Returns
///
/// Returns (pools_recovered, consumer) so the caller can reuse the consumer
/// in the main loop instead of creating a new one (which would replay all messages).
pub async fn bootstrap_pool_cache_from_jetstream(
    nats_client: &NatsClient,
    live_pool_cache: &LivePoolCache,
) -> anyhow::Result<(
    usize,
    Option<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    >,
)> {
    use async_nats::jetstream;
    use futures::StreamExt;

    info!("SLAVE CACHE BOOTSTRAP: Pulling state from JetStream...");

    let jetstream = jetstream::new(nats_client.client().clone());

    // Get stream (must already exist, created by market-data)
    let stream = match jetstream.get_stream(STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, stream = STREAM_NAME, "JetStream stream not found (market-data may not be running)");
            return Ok((0, None));
        }
    };

    // Create ephemeral consumer with LastPerSubject deliver policy
    let consumer_config = slave_consumer_config();
    let consumer = stream.create_consumer(consumer_config).await?;

    let mut pools_recovered = 0;
    let batch_size = 1000;

    // Fetch all available messages in batches until exhausted
    loop {
        let mut messages = consumer.fetch().max_messages(batch_size).messages().await?;
        let mut batch_count = 0;

        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "Error fetching message from JetStream");
                    continue;
                }
            };

            batch_count += 1;

            let pool_update: PoolCacheUpdate = match serde_json::from_slice(&msg.payload) {
                Ok(u) => u,
                Err(e) => {
                    warn!(error = %e, "Failed to deserialize PoolCacheUpdate from JetStream");
                    if let Err(ack_err) = msg.ack().await {
                        warn!(error = %ack_err, "Failed to ack message");
                    }
                    continue;
                }
            };

            if apply_pool_cache_update(live_pool_cache, &pool_update) {
                pools_recovered += 1;
            }

            if let Err(ack_err) = msg.ack().await {
                warn!(error = %ack_err, "Failed to ack message");
            }
        }

        if batch_count < batch_size {
            break;
        }
    }

    info!(pools_recovered, "SLAVE CACHE BOOTSTRAP: Complete");
    Ok((pools_recovered, Some(consumer)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::{
        DexPoolReadiness, POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY,
        POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY, POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY,
        POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY, POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY,
        POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY, POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY,
        POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY, POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY,
        POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY, POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY,
        POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY, POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY,
    };

    /// A.30 Regression: cashback_enabled must be true when JetStream metadata contains
    /// cashback_enabled="true". Verifies pool_cache_sync propagates metadata correctly.
    #[test]
    fn test_jetstream_metadata_propagates_cashback_enabled_true() {
        let cache = LivePoolCache::new();
        let pool_addr = "14Nx7vjtSeMVWugP4zUq5EJkD97ZXKRFUCAPhJJ1pump";
        let base_mint = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
        let quote_mint = "So11111111111111111111111111111111111111112";

        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run-123",
            pool_addr.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_000_000,
            100_000_000,
            Some(0),
            12345,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "creator".to_string(),
            "Creato11111111111111111111111111111111111111".to_string(),
        );
        meta.insert("complete".to_string(), "false".to_string());
        meta.insert(
            "real_token_reserves".to_string(),
            "793100000000000".to_string(),
        );
        meta.insert("real_sol_reserves".to_string(), "0".to_string());
        meta.insert("cashback_enabled".to_string(), "true".to_string());
        update.metadata = Some(meta);

        let applied = apply_pool_cache_update(&cache, &update);
        assert!(applied, "apply_pool_cache_update should succeed");

        let pool_pk = Pubkey::from_str(pool_addr).unwrap();
        let state = cache.get(&pool_pk).expect("cache should have pool");
        match state {
            CachedPoolState::PumpFun(s) => {
                assert!(s.cashback_enabled, "A.30: cashback_enabled must be true when JetStream metadata contains cashback_enabled=\"true\"");
            }
            other => panic!("expected PumpFun state, got {:?}", other),
        }
    }

    #[test]
    fn test_jetstream_metadata_propagates_pump_amm_sell_cashback_remaining() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = "So11111111111111111111111111111111111111112";
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run-456",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        meta.insert(
            "pump_amm_sell_cashback_remaining".to_string(),
            "true".to_string(),
        );
        meta.insert(
            "pump_amm_sell_cashback_third_meta".to_string(),
            Pubkey::new_unique().to_string(),
        );
        update.metadata = Some(meta);
        update.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);

        assert!(apply_pool_cache_update(&cache, &update));
        let (flag, third, t0, t1) = cache.pump_amm_sell_extended_layout(&pool_market);
        assert!(flag);
        assert!(third.is_some());
        assert!(t0.is_none() && t1.is_none());
    }

    #[test]
    fn test_jetstream_metadata_propagates_pump_amm_sell_layout_ready_false() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = "So11111111111111111111111111111111111111112";
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run-456",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        meta.insert(
            "pump_amm_sell_layout_ready".to_string(),
            "false".to_string(),
        );
        update.metadata = Some(meta);
        update.set_dex_readiness_in_metadata(DexPoolReadiness::Partial);

        assert!(apply_pool_cache_update(&cache, &update));
        assert!(
            !cache.pump_amm_sell_layout_ready(&pool_market),
            "SLAVE must preserve authoritative 'SELL layout still unknown' signal"
        );
    }

    #[test]
    fn test_pump_amm_readiness_ready_enables_ready_only_pool_accounts() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        // Fewer than 14 accounts: not legacy-authoritative → no Ready without explicit key.
        let mut meta_short = std::collections::HashMap::new();
        meta_short.insert(
            "pool_accounts".to_string(),
            accounts[..13]
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );

        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        update.metadata = Some(meta_short);
        assert!(apply_pool_cache_update(&cache, &update));
        assert!(cache
            .get_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .is_some());
        assert!(cache
            .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .is_none());

        let mut meta_full = std::collections::HashMap::new();
        meta_full.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            2,
        );
        update.metadata = Some(meta_full);
        update.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &update));
        assert!(cache
            .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .is_some());
    }

    /// P184m / Bug #28+#27: PoolDiscovered with empty `pool_accounts` and 0/0 reserves must not wipe SLAVE row.
    #[test]
    fn test_pump_amm_pool_discovered_empty_accounts_preserves_existing_and_reserves() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        let mut first = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            900,
            2_000,
            None,
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        first.metadata = Some(meta);
        first.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &first));

        let mut second = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            0,
            0,
            None,
            2,
        );
        second.metadata = Some(std::collections::HashMap::new());
        assert!(apply_pool_cache_update(&cache, &second));

        let state = cache.get(&pool_market).expect("pool row");
        let CachedPoolState::PumpAmm(s) = state else {
            panic!("expected PumpAmm");
        };
        assert_eq!(s.pool_accounts.len(), 14);
        assert_eq!(s.base_reserve, Some(900));
        assert_eq!(s.quote_reserve, Some(2_000));
        assert!(cache.pump_amm_quote_ready_by_base_mint(&base_mint));
    }

    /// I-24d / legacy JetStream: struct literal with 14 pool_accounts + non-zero reserves, no readiness key.
    #[test]
    fn test_pump_amm_legacy_authoritative_no_readiness_key_is_ready() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            100,
            200,
            None,
            1,
        );
        update.metadata = Some(meta);
        assert_eq!(
            update.effective_dex_readiness(),
            DexPoolReadiness::Ready,
            "legacy authoritative pump_amm update without dex_pool_readiness key"
        );
        assert!(apply_pool_cache_update(&cache, &update));
        assert!(cache
            .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .is_some());
    }

    #[test]
    fn test_pump_amm_readiness_merge_never_downgrades() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );

        let mut ready_update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        ready_update.metadata = Some(meta.clone());
        ready_update.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &ready_update));
        assert!(cache
            .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .is_some());

        let mut weak_update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            2,
        );
        weak_update.metadata = Some(meta);
        weak_update.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &weak_update));
        assert!(
            cache
                .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
                .is_some(),
            "merge must not downgrade Ready to Observed"
        );
    }

    #[test]
    fn test_pump_amm_authoritative_partial_overwrites_prior_ready() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        let mut ready_meta = std::collections::HashMap::new();
        ready_meta.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        ready_meta.insert("pump_amm_sell_layout_ready".to_string(), "true".to_string());

        let mut ready_update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        ready_update.metadata = Some(ready_meta);
        ready_update.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &ready_update));
        assert!(
            cache.base_mint_has_explicit_pump_amm_ready_pool(&base_mint),
            "authoritative Ready metadata should mark the pool explicitly ready"
        );

        let mut partial_meta = std::collections::HashMap::new();
        partial_meta.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        partial_meta.insert(
            "pump_amm_sell_layout_ready".to_string(),
            "false".to_string(),
        );

        let mut partial_update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            2,
        );
        partial_update.metadata = Some(partial_meta);
        partial_update.set_dex_readiness_in_metadata(DexPoolReadiness::Partial);
        assert!(apply_pool_cache_update(&cache, &partial_update));

        cache.set_pump_amm_sell_layout_ready(&pool_market, true);
        assert!(
            !cache.base_mint_has_explicit_pump_amm_ready_pool(&base_mint),
            "readiness map itself must stay Partial after authoritative overwrite"
        );
    }

    /// Bug #36 (PumpFun): Geyser / partial JetStream must not imply explicit Ready for bonding curve.
    #[test]
    fn test_pumpfun_partial_jetstream_not_explicit_ready() {
        let cache = LivePoolCache::new();
        let pool_addr = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_addr.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_000_000,
            100_000_000,
            Some(0),
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert("creator".to_string(), Pubkey::new_unique().to_string());
        meta.insert(
            "associated_bonding_curve".to_string(),
            Pubkey::new_unique().to_string(),
        );
        meta.insert("complete".to_string(), "false".to_string());
        meta.insert("real_token_reserves".to_string(), "100".to_string());
        meta.insert("real_sol_reserves".to_string(), "200".to_string());
        meta.insert("cashback_enabled".to_string(), "false".to_string());
        update.metadata = Some(meta);
        update.set_dex_readiness_in_metadata(DexPoolReadiness::Partial);

        assert!(apply_pool_cache_update(&cache, &update));
        assert!(
            !cache.pumpfun_bonding_curve_explicitly_ready(&pool_addr),
            "Partial Geyser path must not set explicit Ready"
        );
    }

    /// Bug #36: explicit Ready from cold-path style message; merge Observed must not downgrade.
    #[test]
    fn test_pumpfun_readiness_merge_ready_then_observed_stays_ready() {
        let cache = LivePoolCache::new();
        let pool_addr = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let mut ready_up = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_addr.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            2,
            Some(0),
            1,
        );
        let mut m = std::collections::HashMap::new();
        m.insert("creator".to_string(), Pubkey::new_unique().to_string());
        m.insert(
            "associated_bonding_curve".to_string(),
            Pubkey::new_unique().to_string(),
        );
        m.insert("complete".to_string(), "false".to_string());
        m.insert("real_token_reserves".to_string(), "1".to_string());
        m.insert("real_sol_reserves".to_string(), "2".to_string());
        m.insert("cashback_enabled".to_string(), "false".to_string());
        ready_up.metadata = Some(m.clone());
        ready_up.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &ready_up));
        assert!(cache.pumpfun_bonding_curve_explicitly_ready(&pool_addr));

        let mut weak = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_addr.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            2,
            Some(0),
            2,
        );
        weak.metadata = Some(m);
        weak.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &weak));
        assert!(
            cache.pumpfun_bonding_curve_explicitly_ready(&pool_addr),
            "merge must not downgrade Ready to Observed for PumpFun"
        );
    }

    #[test]
    fn test_pumpfun_balance_updated_refreshes_existing_cache_age() {
        use std::time::Duration;

        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let mut discovered = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_073_000_000_000_000,
            30_000_000_000,
            Some(0),
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert("complete".to_string(), "false".to_string());
        discovered.metadata = Some(meta);
        assert!(apply_pool_cache_update(&cache, &discovered));

        std::thread::sleep(Duration::from_millis(40));
        let (_, _, age_before) = cache.get_with_metadata(&pool).expect("cached");
        assert!(age_before >= 30, "seed row must age before refresh");

        let mut bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_050_000_000_000_000,
            32_000_000_000,
            2,
        );
        bal.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &bal));

        let (_, _, age_after) = cache.get_with_metadata(&pool).expect("cached");
        assert!(
            age_after < 20,
            "PumpFun BalanceUpdated with reserve basis must refresh SLAVE cache age"
        );
    }

    #[test]
    fn pumpfun_balance_updated_same_reserves_new_slot_refreshes_slave_age() {
        use std::time::Duration;

        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let token_reserves = 1_073_000_000_000_000u64;
        let sol_reserves = 30_000_000_000u64;

        let mut discovered = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            token_reserves,
            sol_reserves,
            Some(0),
            10,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert("complete".to_string(), "false".to_string());
        discovered.metadata = Some(meta);
        assert!(apply_pool_cache_update(&cache, &discovered));

        std::thread::sleep(Duration::from_millis(40));
        let (_, slot_before, age_before) = cache.get_with_metadata(&pool).expect("cached");
        assert_eq!(slot_before, 10);
        assert!(
            age_before >= 30,
            "seed row must age before slot-only refresh"
        );

        let bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            token_reserves,
            sol_reserves,
            11,
        );
        assert!(apply_pool_cache_update(&cache, &bal));

        let (_, slot_after, age_after) = cache.get_with_metadata(&pool).expect("cached");
        assert_eq!(
            slot_after, slot_before,
            "identical reserves must not advance material slot"
        );
        assert!(
            age_after >= age_before,
            "identical reserves must not reset SLAVE cache age"
        );
        assert_eq!(
            cache.get_last_seen_slot(&pool),
            Some(11),
            "last_seen_slot tracks Geyser heartbeat"
        );
    }

    #[test]
    fn test_raydium_cpmm_balance_updated_preserves_vaults_and_merges_readiness() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let v0 = Pubkey::new_unique();
        let v1 = Pubkey::new_unique();

        let mut discovered = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "raydium_cpmm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            0,
            0,
            None,
            1,
        );
        let mut m = std::collections::HashMap::new();
        m.insert(
            POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY.to_string(),
            format!("{v0},{v1}"),
        );
        discovered.metadata = Some(m);
        discovered.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &discovered));

        let mut bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "raydium_cpmm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_000_000,
            50_000_000_000,
            2,
        );
        bal.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::RaydiumCpmm(s) => {
                assert_eq!(s.token_0_vault, v0);
                assert_eq!(s.token_1_vault, v1);
                assert_eq!(s.reserve_0, Some(1_000_000));
                assert_eq!(s.reserve_1, Some(50_000_000_000));
            }
            other => panic!("expected RaydiumCpmm, got {:?}", other),
        }
        assert!(cache.raydium_cpmm_pool_explicitly_ready(&pool));
    }

    /// Legacy SLAVE state: default vaults from old JetStream messages must not block metadata vaults.
    #[test]
    fn test_raydium_cpmm_balance_updated_metadata_vaults_when_existing_defaults() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let v0 = Pubkey::new_unique();
        let v1 = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote_mint,
                token_0_vault: Pubkey::default(),
                token_1_vault: Pubkey::default(),
                reserve_0: Some(100),
                reserve_1: Some(200),
            }),
            1,
        );

        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY.to_string(),
            format!("{v0},{v1}"),
        );
        let mut bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "raydium_cpmm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_000_000,
            50_000_000_000,
            2,
        );
        bal.metadata = Some(meta);
        bal.set_dex_readiness_in_metadata(DexPoolReadiness::Partial);
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::RaydiumCpmm(s) => {
                assert_eq!(s.token_0_vault, v0);
                assert_eq!(s.token_1_vault, v1);
                assert_ne!(s.token_0_vault, Pubkey::default());
                assert_ne!(s.token_1_vault, Pubkey::default());
            }
            other => panic!("expected RaydiumCpmm, got {:?}", other),
        }
        assert_eq!(
            cache.raydium_cpmm_readiness(&pool),
            Some(DexPoolReadiness::Partial)
        );
    }

    /// On-chain `token_0 == SOL`: publisher sends normalized base/quote mints and base/quote vault order in metadata.
    #[test]
    fn test_raydium_cpmm_pool_discovered_metadata_vaults_match_normalized_mints_when_sol_is_token_0(
    ) {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let sol = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let v_non_sol_vault = Pubkey::new_unique();
        let v_sol_vault = Pubkey::new_unique();

        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "raydium_cpmm".to_string(),
            base_mint.to_string(),
            sol.to_string(),
            1_000_000,
            50_000_000_000,
            None,
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY.to_string(),
            format!("{v_non_sol_vault},{v_sol_vault}"),
        );
        update.metadata = Some(meta);
        update.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &update));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::RaydiumCpmm(s) => {
                assert_eq!(s.token_0_mint, base_mint);
                assert_eq!(s.token_1_mint, sol);
                assert_eq!(s.token_0_vault, v_non_sol_vault);
                assert_eq!(s.token_1_vault, v_sol_vault);
            }
            other => panic!("expected RaydiumCpmm, got {:?}", other),
        }
    }

    /// Scope 12: `meteora_cpmm` JetStream minimal state must use `CachedPoolState::MeteoraCpmm`,
    /// not DLMM-shaped `Meteora`.
    #[test]
    fn test_meteora_cpmm_pool_discovered_builds_meteora_cpmm_variant() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let token_0 = Pubkey::new_unique();
        let token_1 = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_cpmm".to_string(),
            token_0.to_string(),
            token_1.to_string(),
            1_000_000,
            50_000_000_000,
            None,
            1,
        );
        assert!(apply_pool_cache_update(&cache, &update));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::MeteoraCpmm(s) => {
                assert_eq!(s.token_0_mint, token_0);
                assert_eq!(s.token_1_mint, token_1);
                assert_eq!(s.reserve_0, 1_000_000);
                assert_eq!(s.reserve_1, 50_000_000_000);
            }
            other => panic!("expected MeteoraCpmm, got {:?}", other),
        }
    }

    #[test]
    fn test_meteora_dlmm_pool_discovered_stays_meteora_dlmm_variant() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let token_x = Pubkey::new_unique();
        let token_y = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_dlmm".to_string(),
            token_x.to_string(),
            token_y.to_string(),
            100,
            200,
            None,
            1,
        );
        assert!(apply_pool_cache_update(&cache, &update));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::Meteora(s) => {
                assert_eq!(s.token_x_mint, token_x);
                assert_eq!(s.token_y_mint, token_y);
                assert_eq!(s.reserve_x_balance, Some(100));
                assert_eq!(s.reserve_y_balance, Some(200));
            }
            other => panic!("expected Meteora (DLMM), got {:?}", other),
        }
    }

    /// JetStream `PoolDiscovered` with DLMM metadata must reconstruct vaults + static fields.
    #[test]
    fn test_meteora_dlmm_pool_discovered_metadata_reconstructs_shape() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let token_x = Pubkey::new_unique();
        let token_y = Pubkey::new_unique();
        let vx = Pubkey::new_unique();
        let vy = Pubkey::new_unique();

        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_dlmm".to_string(),
            token_x.to_string(),
            token_y.to_string(),
            1,
            2,
            None,
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY.to_string(),
            format!("{vx},{vy}"),
        );
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY.to_string(),
            "-42".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY.to_string(),
            "100".to_string(),
        );
        update.metadata = Some(meta);

        assert!(apply_pool_cache_update(&cache, &update));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::Meteora(s) => {
                assert_eq!(s.reserve_x, vx);
                assert_eq!(s.reserve_y, vy);
                assert_eq!(s.active_id, -42);
                assert_eq!(s.bin_step, 100);
                assert_eq!(s.reserve_x_balance, Some(1));
                assert_eq!(s.reserve_y_balance, Some(2));
            }
            other => panic!("expected Meteora (DLMM), got {:?}", other),
        }
    }

    /// `active_id == 0` and `bin_step == 0` are valid on-chain; BalanceUpdated without DLMM keys must not clobber them.
    #[test]
    fn test_meteora_dlmm_balance_updated_without_metadata_preserves_zero_static_fields() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let token_x = Pubkey::new_unique();
        let token_y = Pubkey::new_unique();
        let vx = Pubkey::new_unique();
        let vy = Pubkey::new_unique();

        let mut disc = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_dlmm".to_string(),
            token_x.to_string(),
            token_y.to_string(),
            10,
            20,
            None,
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY.to_string(),
            format!("{vx},{vy}"),
        );
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY.to_string(),
            "0".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY.to_string(),
            "0".to_string(),
        );
        disc.metadata = Some(meta);
        assert!(apply_pool_cache_update(&cache, &disc));

        let bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_dlmm".to_string(),
            token_x.to_string(),
            token_y.to_string(),
            0,
            99,
            2,
        );
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::Meteora(s) => {
                assert_eq!(s.active_id, 0);
                assert_eq!(s.bin_step, 0);
                assert_eq!(s.reserve_x, vx);
                assert_eq!(s.reserve_y, vy);
                assert_eq!(s.reserve_x_balance, Some(10));
                assert_eq!(s.reserve_y_balance, Some(99));
            }
            other => panic!("expected Meteora (DLMM), got {:?}", other),
        }
    }

    /// BalanceUpdated without metadata must keep prior non-zero static fields (not re-derived from unwrap defaults).
    #[test]
    fn test_meteora_dlmm_balance_updated_preserves_static_when_metadata_absent() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let token_x = Pubkey::new_unique();
        let token_y = Pubkey::new_unique();
        let vx = Pubkey::new_unique();
        let vy = Pubkey::new_unique();

        let mut disc = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_dlmm".to_string(),
            token_x.to_string(),
            token_y.to_string(),
            5,
            6,
            None,
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_DLMM_VAULTS_KEY.to_string(),
            format!("{vx},{vy}"),
        );
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_DLMM_ACTIVE_ID_KEY.to_string(),
            "7".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_DLMM_BIN_STEP_KEY.to_string(),
            "8".to_string(),
        );
        disc.metadata = Some(meta);
        assert!(apply_pool_cache_update(&cache, &disc));

        let bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_dlmm".to_string(),
            token_x.to_string(),
            token_y.to_string(),
            100,
            200,
            2,
        );
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::Meteora(s) => {
                assert_eq!(s.active_id, 7);
                assert_eq!(s.bin_step, 8);
                assert_eq!(s.reserve_x, vx);
                assert_eq!(s.reserve_y, vy);
            }
            other => panic!("expected Meteora (DLMM), got {:?}", other),
        }
    }

    /// Partial BalanceUpdated (one vault) must merge with prior reserves for Meteora CPMM.
    #[test]
    fn test_meteora_cpmm_balance_updated_merges_single_side_reserve() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let token_0 = Pubkey::new_unique();
        let token_1 = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let disc = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_cpmm".to_string(),
            token_0.to_string(),
            token_1.to_string(),
            1_000_000,
            2_000_000,
            None,
            1,
        );
        assert!(apply_pool_cache_update(&cache, &disc));

        let bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_cpmm".to_string(),
            token_0.to_string(),
            token_1.to_string(),
            0,
            9_999_999,
            2,
        );
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::MeteoraCpmm(s) => {
                assert_eq!(
                    s.reserve_0, 1_000_000,
                    "token_0 side must be preserved when update has 0"
                );
                assert_eq!(s.reserve_1, 9_999_999);
            }
            other => panic!("expected MeteoraCpmm, got {:?}", other),
        }
    }

    /// Wallet-bootstrap JetStream publishes `meteora_cpmm_vaults` in normalized base/quote order;
    /// SLAVE merge must map them onto on-chain token_0/token_1 vault fields.
    #[test]
    fn test_meteora_cpmm_balance_updated_metadata_vaults_normalized_order() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let base_vault = Pubkey::new_unique();
        let quote_vault = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::MeteoraCpmm(MeteoraCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote_mint,
                token_0_vault: Pubkey::default(),
                token_1_vault: Pubkey::default(),
                amm_config: Pubkey::default(),
                observation_key: Pubkey::default(),
                token_0_program: Pubkey::default(),
                token_1_program: Pubkey::default(),
                reserve_0: 100,
                reserve_1: 200,
                mint_0_decimals: 0,
                mint_1_decimals: 0,
                status: 0,
            }),
            1,
        );

        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY.to_string(),
            format!("{base_vault},{quote_vault}"),
        );
        let mut bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_cpmm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            300,
            400,
            2,
        );
        bal.metadata = Some(meta);
        bal.set_dex_readiness_in_metadata(DexPoolReadiness::Partial);
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::MeteoraCpmm(s) => {
                assert_eq!(s.token_0_vault, base_vault);
                assert_eq!(s.token_1_vault, quote_vault);
                assert_eq!(s.reserve_0, 300);
                assert_eq!(s.reserve_1, 400);
            }
            other => panic!("expected MeteoraCpmm, got {:?}", other),
        }
    }

    /// SOL is on-chain `token_0`; JetStream uses normalized `base_mint` / `quote_mint` and
    /// `meteora_cpmm_vaults` = `base_vault,quote_vault` for that pair. Merge must assign
    /// `quote_vault` → `token_0_vault` (SOL) and `base_vault` → `token_1_vault` without applying
    /// the old SOL-heuristic swap on top of already-normalized metadata (double-swap bug).
    #[test]
    fn test_meteora_cpmm_balance_updated_metadata_vaults_sol_token_0_swap() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let sol = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let base_vault = Pubkey::new_unique();
        let quote_vault = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::MeteoraCpmm(MeteoraCpmmState {
                token_0_mint: sol,
                token_1_mint: base_mint,
                token_0_vault: Pubkey::default(),
                token_1_vault: Pubkey::default(),
                amm_config: Pubkey::default(),
                observation_key: Pubkey::default(),
                token_0_program: Pubkey::default(),
                token_1_program: Pubkey::default(),
                reserve_0: 1,
                reserve_1: 2,
                mint_0_decimals: 0,
                mint_1_decimals: 0,
                status: 0,
            }),
            1,
        );

        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY.to_string(),
            format!("{base_vault},{quote_vault}"),
        );
        let mut bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_cpmm".to_string(),
            base_mint.to_string(),
            sol.to_string(),
            10,
            20,
            2,
        );
        bal.metadata = Some(meta);
        bal.set_dex_readiness_in_metadata(DexPoolReadiness::Partial);
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::MeteoraCpmm(s) => {
                assert_eq!(s.token_0_mint, sol);
                assert_eq!(s.token_1_mint, base_mint);
                assert_eq!(s.reserve_0, 20, "SOL leg reserve (quote in update)");
                assert_eq!(s.reserve_1, 10, "base leg reserve");
                assert_eq!(s.token_0_vault, quote_vault, "SOL / quote_mint vault");
                assert_eq!(s.token_1_vault, base_vault, "base_mint vault");
            }
            other => panic!("expected MeteoraCpmm, got {:?}", other),
        }
    }

    /// PR #53 follow-up: LastPerSubject bootstrap may replay only `BalanceUpdated` with no prior
    /// SLAVE row. `meteora_cpmm_onchain_mints` + normalized vault metadata must reconstruct true
    /// on-chain token_0/token_1, reserves, and vaults when SOL is token_0.
    #[test]
    fn test_meteora_cpmm_balance_updated_cold_bootstrap_sol_token_0_no_existing_cache() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let sol = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let base_vault = Pubkey::new_unique();
        let quote_vault = Pubkey::new_unique();

        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_CPMM_VAULTS_KEY.to_string(),
            format!("{base_vault},{quote_vault}"),
        );
        meta.insert(
            POOL_CACHE_UPDATE_METEORA_CPMM_ONCHAIN_MINTS_KEY.to_string(),
            format!("{sol},{base_mint}"),
        );
        let mut bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_cpmm".to_string(),
            base_mint.to_string(),
            sol.to_string(),
            100,
            50_000_000_000,
            2,
        );
        bal.metadata = Some(meta);
        bal.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::MeteoraCpmm(s) => {
                assert_eq!(s.token_0_mint, sol);
                assert_eq!(s.token_1_mint, base_mint);
                assert_eq!(s.reserve_0, 50_000_000_000, "SOL reserve on token_0 leg");
                assert_eq!(s.reserve_1, 100, "base mint reserve on token_1 leg");
                assert_eq!(s.token_0_vault, quote_vault);
                assert_eq!(s.token_1_vault, base_vault);
            }
            other => panic!("expected MeteoraCpmm, got {:?}", other),
        }
    }

    /// After Geyser-filled vaults/decimals, JetStream BalanceUpdated must not zero them.
    #[test]
    fn test_meteora_cpmm_balance_updated_preserves_geyser_static_fields() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let token_0 = Pubkey::new_unique();
        let token_1 = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let v0 = Pubkey::new_unique();
        let v1 = Pubkey::new_unique();
        let amm_cfg = Pubkey::new_unique();
        let obs = Pubkey::new_unique();
        let p0 = Pubkey::new_unique();
        let p1 = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::MeteoraCpmm(MeteoraCpmmState {
                token_0_mint: token_0,
                token_1_mint: token_1,
                token_0_vault: v0,
                token_1_vault: v1,
                amm_config: amm_cfg,
                observation_key: obs,
                token_0_program: p0,
                token_1_program: p1,
                reserve_0: 100,
                reserve_1: 200,
                mint_0_decimals: 6,
                mint_1_decimals: 9,
                status: 1,
            }),
            1,
        );

        let bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_cpmm".to_string(),
            token_0.to_string(),
            token_1.to_string(),
            111,
            0,
            2,
        );
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::MeteoraCpmm(s) => {
                assert_eq!(s.token_0_vault, v0);
                assert_eq!(s.token_1_vault, v1);
                assert_eq!(s.amm_config, amm_cfg);
                assert_eq!(s.observation_key, obs);
                assert_eq!(s.token_0_program, p0);
                assert_eq!(s.token_1_program, p1);
                assert_eq!(s.mint_0_decimals, 6);
                assert_eq!(s.mint_1_decimals, 9);
                assert_eq!(s.status, 1);
                assert_eq!(s.reserve_0, 111);
                assert_eq!(s.reserve_1, 200);
            }
            other => panic!("expected MeteoraCpmm, got {:?}", other),
        }
    }

    #[test]
    fn test_raydium_cpmm_readiness_merge_never_downgrades() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let v0 = Pubkey::new_unique();
        let v1 = Pubkey::new_unique();

        let mut m = std::collections::HashMap::new();
        m.insert(
            POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY.to_string(),
            format!("{v0},{v1}"),
        );

        let mut ready_up = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "raydium_cpmm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        ready_up.metadata = Some(m.clone());
        ready_up.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &ready_up));
        assert!(cache.raydium_cpmm_pool_explicitly_ready(&pool));

        let mut weak = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "raydium_cpmm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            2,
        );
        weak.metadata = Some(m);
        weak.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &weak));
        assert!(
            cache.raydium_cpmm_pool_explicitly_ready(&pool),
            "merge must not downgrade Ready to Observed for Raydium CPMM"
        );
    }

    /// BalanceUpdated for `raydium` omits metadata: SLAVE must keep Serum static accounts from
    /// existing state so readiness does not spuriously downgrade.
    #[test]
    fn test_raydium_amm_balance_updated_preserves_serum_and_merges_readiness() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let market_id = Pubkey::new_unique();
        let bids = Pubkey::new_unique();
        let asks = Pubkey::new_unique();
        let eq = Pubkey::new_unique();

        let mut meta = std::collections::HashMap::new();
        meta.insert("market_id".to_string(), market_id.to_string());
        meta.insert("serum_bids".to_string(), bids.to_string());
        meta.insert("serum_asks".to_string(), asks.to_string());
        meta.insert("serum_event_queue".to_string(), eq.to_string());

        let mut disc = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "raydium".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_000_000,
            50_000_000_000,
            None,
            1,
        );
        disc.metadata = Some(meta);
        disc.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &disc));
        assert!(cache.raydium_amm_pool_explicitly_ready(&pool));
        assert_eq!(
            cache.raydium_amm_readiness(&pool),
            Some(DexPoolReadiness::Ready)
        );

        let mut bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "raydium".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            2_000_000,
            60_000_000_000,
            2,
        );
        bal.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::RaydiumAmm(s) => {
                assert_eq!(s.market_id, market_id);
                assert_eq!(s.serum_bids, Some(bids));
                assert_eq!(s.serum_asks, Some(asks));
                assert_eq!(s.serum_event_queue, Some(eq));
            }
            other => panic!("expected RaydiumAmm, got {:?}", other),
        }
        assert!(
            cache.raydium_amm_pool_explicitly_ready(&pool),
            "merge must not downgrade Ready to Observed for Raydium AMM"
        );
        assert_eq!(
            cache.raydium_amm_readiness(&pool),
            Some(DexPoolReadiness::Ready)
        );
    }

    #[test]
    fn test_meteora_cpmm_readiness_merge_never_downgrades() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let mut ready_up = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_cpmm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        ready_up.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &ready_up));
        assert!(cache.meteora_cpmm_pool_explicitly_ready(&pool));
        assert_eq!(
            cache.meteora_cpmm_readiness(&pool),
            Some(DexPoolReadiness::Ready)
        );

        let mut weak = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_cpmm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            2,
        );
        weak.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &weak));
        assert!(
            cache.meteora_cpmm_pool_explicitly_ready(&pool),
            "merge must not downgrade Ready to Observed for Meteora CPMM"
        );
        assert_eq!(
            cache.meteora_cpmm_readiness(&pool),
            Some(DexPoolReadiness::Ready)
        );
    }

    #[test]
    fn test_meteora_dlmm_readiness_merge_never_downgrades() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let mut ready_up = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_dlmm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        ready_up.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &ready_up));
        assert!(cache.meteora_dlmm_pool_explicitly_ready(&pool));
        assert_eq!(
            cache.meteora_dlmm_readiness(&pool),
            Some(DexPoolReadiness::Ready)
        );

        let mut weak = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "meteora_dlmm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            2,
        );
        weak.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &weak));
        assert!(
            cache.meteora_dlmm_pool_explicitly_ready(&pool),
            "merge must not downgrade Ready to Observed for Meteora DLMM"
        );
        assert_eq!(
            cache.meteora_dlmm_readiness(&pool),
            Some(DexPoolReadiness::Ready)
        );
    }

    #[test]
    fn test_orca_pool_discovered_jetstream_reconstructs_static_fields() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let va = Pubkey::new_unique();
        let vb = Pubkey::new_unique();

        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY.to_string(),
            format!("{va},{vb}"),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY.to_string(),
            "-7".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY.to_string(),
            "64".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY.to_string(),
            "12345678901234567890".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY.to_string(),
            "999888777666".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY.to_string(),
            "3000".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY.to_string(),
            "300".to_string(),
        );

        let mut disc = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "orca".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            5,
            6,
            None,
            1,
        );
        disc.metadata = Some(meta);
        disc.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &disc));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::Orca(s) => {
                assert_eq!(s.token_mint_a, base_mint);
                assert_eq!(s.token_mint_b, quote_mint);
                assert_eq!(s.token_vault_a, va);
                assert_eq!(s.token_vault_b, vb);
                assert_eq!(s.tick_current_index, -7);
                assert_eq!(s.tick_spacing, 64);
                assert_eq!(s.sqrt_price, 12_345_678_901_234_567_890u128);
                assert_eq!(s.liquidity, 999_888_777_666);
                assert_eq!(s.fee_rate, 3000);
                assert_eq!(s.protocol_fee_rate, 300);
                assert_eq!(s.vault_a_balance, Some(5));
                assert_eq!(s.vault_b_balance, Some(6));
            }
            other => panic!("expected Orca, got {:?}", other),
        }
        assert!(cache.orca_pool_explicitly_ready(&pool));
    }

    /// `tick_current_index == 0` is valid; BalanceUpdated without tick metadata must not overwrite from a bogus default.
    #[test]
    fn test_orca_balance_updated_preserves_tick_zero_when_metadata_omits_tick() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let va = Pubkey::new_unique();
        let vb = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::Orca(OrcaWhirlpoolState {
                token_mint_a: base_mint,
                token_mint_b: quote_mint,
                token_vault_a: va,
                token_vault_b: vb,
                tick_current_index: 0,
                sqrt_price: 42,
                liquidity: 100,
                fee_rate: 1,
                protocol_fee_rate: 2,
                tick_spacing: 8,
                vault_a_balance: Some(10),
                vault_b_balance: Some(20),
                token_a_program: None,
                token_b_program: None,
            }),
            1,
        );

        let mut bal = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "orca".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            11,
            21,
            2,
        );
        bal.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &bal));

        match cache.get(&pool).expect("pool in cache") {
            CachedPoolState::Orca(s) => {
                assert_eq!(s.tick_current_index, 0);
                assert_eq!(s.sqrt_price, 42);
                assert_eq!(s.tick_spacing, 8);
                assert_eq!(s.vault_a_balance, Some(11));
                assert_eq!(s.vault_b_balance, Some(21));
            }
            other => panic!("expected Orca, got {:?}", other),
        }
    }

    #[test]
    fn test_orca_readiness_merge_never_downgrades() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let va = Pubkey::new_unique();
        let vb = Pubkey::new_unique();

        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY.to_string(),
            format!("{va},{vb}"),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_TICK_CURRENT_INDEX_KEY.to_string(),
            "1".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_TICK_SPACING_KEY.to_string(),
            "8".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_SQRT_PRICE_KEY.to_string(),
            "100".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_LIQUIDITY_KEY.to_string(),
            "200".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_FEE_RATE_KEY.to_string(),
            "1".to_string(),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_PROTOCOL_FEE_RATE_KEY.to_string(),
            "2".to_string(),
        );

        let mut ready_up = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "orca".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        ready_up.metadata = Some(meta.clone());
        ready_up.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &ready_up));
        assert!(cache.orca_pool_explicitly_ready(&pool));

        let mut weak = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "orca".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            2,
        );
        weak.metadata = Some(meta);
        weak.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &weak));
        assert!(
            cache.orca_pool_explicitly_ready(&pool),
            "merge must not downgrade Ready to Observed for Orca"
        );
        assert_eq!(cache.orca_readiness(&pool), Some(DexPoolReadiness::Ready));
    }

    #[test]
    fn test_orca_balance_updated_bootstrap_maps_normalized_reserves_when_sol_is_token_a() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let usdc = Pubkey::new_unique();
        let sol = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let va = Pubkey::new_unique();
        let vb = Pubkey::new_unique();

        let mut meta = std::collections::HashMap::new();
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_WHIRLPOOL_VAULTS_KEY.to_string(),
            format!("{va},{vb}"),
        );
        meta.insert(
            POOL_CACHE_UPDATE_ORCA_ONCHAIN_MINTS_KEY.to_string(),
            format!("{sol},{usdc}"),
        );

        let mut update = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "orca".to_string(),
            usdc.to_string(),
            sol.to_string(),
            66_000_000,
            1_100_000_000,
            2,
        );
        update.metadata = Some(meta);
        assert!(apply_pool_cache_update(&cache, &update));

        let state = cache.get(&pool).expect("orca pool");
        if let CachedPoolState::Orca(s) = state {
            assert_eq!(s.token_mint_a, sol);
            assert_eq!(s.token_mint_b, usdc);
            assert_eq!(s.vault_a_balance, Some(1_100_000_000));
            assert_eq!(s.vault_b_balance, Some(66_000_000));
        } else {
            panic!("expected Orca state");
        }
    }

    #[test]
    fn test_orca_balance_updated_maps_normalized_reserves_when_sol_is_token_a() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let usdc = Pubkey::new_unique();
        let sol = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let va = Pubkey::new_unique();
        let vb = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::Orca(OrcaWhirlpoolState {
                token_mint_a: sol,
                token_mint_b: usdc,
                token_vault_a: va,
                token_vault_b: vb,
                tick_current_index: 0,
                sqrt_price: 1,
                liquidity: 1,
                fee_rate: 1,
                protocol_fee_rate: 1,
                tick_spacing: 64,
                vault_a_balance: Some(1_000_000_000),
                vault_b_balance: Some(65_000_000),
                token_a_program: None,
                token_b_program: None,
            }),
            1,
        );

        let update = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "orca".to_string(),
            usdc.to_string(),
            sol.to_string(),
            66_000_000,
            1_100_000_000,
            2,
        );
        assert!(apply_pool_cache_update(&cache, &update));

        let state = cache.get(&pool).expect("orca pool");
        if let CachedPoolState::Orca(s) = state {
            assert_eq!(s.token_mint_a, sol);
            assert_eq!(s.token_mint_b, usdc);
            assert_eq!(s.vault_a_balance, Some(1_100_000_000));
            assert_eq!(s.vault_b_balance, Some(66_000_000));
        } else {
            panic!("expected Orca state");
        }
    }

    /// P0-B: Ensure-equivalent JetStream publish with unchanged reserves must not spoof material slot.
    #[test]
    fn ensure_equivalent_pool_cache_update_advances_slave_slot() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let token_reserves = 1_073_000_000_000_000u64;
        let sol_reserves = 30_000_000_000u64;

        let mut seed = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            token_reserves,
            sol_reserves,
            Some(0),
            436_771_116,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert("complete".to_string(), "false".to_string());
        seed.metadata = Some(meta);
        assert!(apply_pool_cache_update(&cache, &seed));

        let ensure_refresh = PoolCacheUpdate::new_balance_updated(
            "market-data",
            "0.1.0",
            "run",
            pool.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            token_reserves,
            sol_reserves,
            436_771_200,
        );
        assert!(
            apply_pool_cache_update(&cache, &ensure_refresh),
            "Ensure-equivalent BalanceUpdated must apply when slot advances"
        );
        let (_, slot_after, _) = cache.get_with_metadata(&pool).expect("cached");
        assert_eq!(
            slot_after, 436_771_116,
            "unchanged reserves must preserve material slot despite newer Geyser slot"
        );
        assert!(
            cache
                .get_last_seen_slot(&pool)
                .is_some_and(|s| s >= 436_771_200),
            "last_seen_slot must track newer Geyser slot"
        );
    }

    #[test]
    fn apply_stale_lower_slot_sustains_age_when_reserve_basis() {
        use std::time::Duration;

        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let fresh = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_000,
            2_000,
            500,
        );
        assert!(apply_pool_cache_update(&cache, &fresh));

        std::thread::sleep(Duration::from_millis(40));

        let stale = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_000,
            2_000,
            400,
        );
        assert!(
            apply_pool_cache_update(&cache, &stale),
            "stale-slot heartbeat with reserve basis must sustain age (C1h3)"
        );
        let (_, slot, age_ms) = cache.get_with_metadata(&pool).expect("cached");
        assert_eq!(slot, 500, "slot must not regress");
        assert!(age_ms < 20, "age must refresh on stale-slot sustain");
    }

    #[test]
    fn apply_rejects_stale_lower_slot_without_reserve_basis() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let discovered = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            0,
            0,
            None,
            500,
        );
        assert!(apply_pool_cache_update(&cache, &discovered));

        let stale = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            0,
            0,
            400,
        );
        assert!(
            !apply_pool_cache_update(&cache, &stale),
            "0/0 stale slot must not spoof age refresh"
        );
        let (_, slot, _) = cache.get_with_metadata(&pool).expect("cached");
        assert_eq!(slot, 500);
    }

    #[test]
    fn apply_pool_state_update_bridge_advances_pumpfun_slave_slot() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let token_reserves = 1_073_000_000_000_000u64;
        let sol_reserves = 30_000_000_000u64;

        let mut seed = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            token_reserves,
            sol_reserves,
            Some(0),
            436_803_351,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert("complete".to_string(), "false".to_string());
        seed.metadata = Some(meta);
        assert!(apply_pool_cache_update(&cache, &seed));

        let outcome = apply_pool_state_update_to_cache(
            &cache,
            &PoolStateUpdateBridge {
                pool_address: &pool.to_string(),
                dex: "pumpfun",
                reserve_base: token_reserves.saturating_sub(1_000_000),
                reserve_quote: sol_reserves.saturating_add(1_000_000),
                base_mint: &base_mint.to_string(),
                quote_mint: &quote_mint.to_string(),
                update_slot: 436_803_366,
            },
        );
        assert_eq!(outcome, PoolCacheApplyOutcome::Applied);
        let (_, slot_after, _) = cache.get_with_metadata(&pool).expect("cached");
        assert!(
            slot_after >= 436_803_366,
            "Core PoolStateUpdate bridge must raise SLAVE slot to >= publish_slot S"
        );
    }

    #[test]
    fn apply_skips_restored_dex_without_reject() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let update = PoolCacheUpdate::new_balance_updated(
            "test",
            "0.1.0",
            "run",
            pool.to_string(),
            "restored".to_string(),
            Pubkey::new_unique().to_string(),
            Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT)
                .unwrap()
                .to_string(),
            1_000,
            2_000,
            100,
        );
        assert_eq!(
            apply_pool_cache_update_outcome(&cache, &update),
            PoolCacheApplyOutcome::SkippedRestoredDex
        );
        assert!(cache.get(&pool).is_none());
    }
}
