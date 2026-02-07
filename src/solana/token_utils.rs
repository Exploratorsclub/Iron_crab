use crate::execution::live_pool_cache::LivePoolCache;
use crate::metrics::{
    MINT_DECIMALS_FALLBACK_DEFAULT, MINT_DECIMALS_SOURCE_ACCOUNT, MINT_DECIMALS_SOURCE_CACHE,
    MINT_DECIMALS_SOURCE_SUPPLY,
};
use crate::solana::rpc::SolanaRpc;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey as SdkPubkey;
use tracing::warn;

/// Fetch SPL token decimals with robust fallbacks and metrics.
///
/// Lookup order (GEYSER-FIRST):
/// 1. LivePoolCache (populated from Geyser TokenMintInfo – 0ms, no RPC)
/// 2. RPC getTokenSupply (cheap & authoritative)
/// 3. RPC getAccount raw mint layout offset 44
/// 4. Default 0 with warning
///
/// When a value is resolved via RPC and a cache is available, the result is
/// written back into the cache so subsequent calls are instant.
pub async fn get_token_decimals_or_default(
    rpc: &SolanaRpc,
    mint: &SdkPubkey,
    cache: Option<&LivePoolCache>,
) -> u8 {
    // 1. Try LivePoolCache first (Geyser-populated, zero latency)
    if let Some(c) = cache {
        if let Some(d) = c.get_mint_decimals(mint) {
            MINT_DECIMALS_SOURCE_CACHE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return d;
        }
    }

    // 2. RPC: getTokenSupply (single RPC call, returns decimals directly)
    if let Ok(supply) = rpc.rpc.get_token_supply(mint).await {
        MINT_DECIMALS_SOURCE_SUPPLY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Write back to cache so next lookup is free
        if let Some(c) = cache {
            c.set_mint_decimals(*mint, supply.decimals);
        }
        return supply.decimals;
    }

    // 3. RPC: getAccount raw mint layout (decimals at offset 44)
    match rpc.rpc.get_account(mint).await {
        Ok(acct) if acct.data.len() > 44 => {
            MINT_DECIMALS_SOURCE_ACCOUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let decimals = acct.data[44];
            if let Some(c) = cache {
                c.set_mint_decimals(*mint, decimals);
            }
            decimals
        }
        _ => {
            MINT_DECIMALS_FALLBACK_DEFAULT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(mint = %mint, "mint decimals unknown; falling back to 0");
            0
        }
    }
}

/// Try to fetch decimals returning Result, using the same logic, without defaulting.
///
/// Lookup order: LivePoolCache → RPC getTokenSupply → RPC getAccount[44]
pub async fn try_token_decimals(
    rpc: &SolanaRpc,
    mint: &SdkPubkey,
    cache: Option<&LivePoolCache>,
) -> Result<u8> {
    // 1. Try LivePoolCache first
    if let Some(c) = cache {
        if let Some(d) = c.get_mint_decimals(mint) {
            MINT_DECIMALS_SOURCE_CACHE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(d);
        }
    }

    // 2. RPC: getTokenSupply
    if let Ok(supply) = rpc.rpc.get_token_supply(mint).await {
        MINT_DECIMALS_SOURCE_SUPPLY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(c) = cache {
            c.set_mint_decimals(*mint, supply.decimals);
        }
        return Ok(supply.decimals);
    }

    // 3. RPC: getAccount raw
    let acct = rpc.rpc.get_account(mint).await?;
    if acct.data.len() > 44 {
        MINT_DECIMALS_SOURCE_ACCOUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let decimals = acct.data[44];
        if let Some(c) = cache {
            c.set_mint_decimals(*mint, decimals);
        }
        Ok(decimals)
    } else {
        Err(anyhow!("mint account data too short to read decimals"))
    }
}
