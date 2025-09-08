use crate::metrics::{
    MINT_DECIMALS_FALLBACK_DEFAULT, MINT_DECIMALS_SOURCE_ACCOUNT, MINT_DECIMALS_SOURCE_SUPPLY,
};
use crate::solana::rpc::SolanaRpc;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey as SdkPubkey;
use tracing::warn;

/// Fetch SPL token decimals with robust fallbacks and metrics.
/// Order: getTokenSupply.decimals -> raw mint account[44] -> default 0 with warning.
pub async fn get_token_decimals_or_default(rpc: &SolanaRpc, mint: &SdkPubkey) -> u8 {
    // Prefer RPC supply (cheap & authoritative)
    if let Ok(supply) = rpc.rpc.get_token_supply(mint).await {
        MINT_DECIMALS_SOURCE_SUPPLY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return supply.decimals;
    }
    // Fallback to mint account layout (decimals at offset 44)
    match rpc.rpc.get_account(mint).await {
        Ok(acct) if acct.data.len() > 44 => {
            MINT_DECIMALS_SOURCE_ACCOUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            acct.data[44]
        }
        _ => {
            MINT_DECIMALS_FALLBACK_DEFAULT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(mint = %mint, "mint decimals unknown; falling back to 0");
            0
        }
    }
}

/// Try to fetch decimals returning Result, using the same logic, without defaulting.
pub async fn try_token_decimals(rpc: &SolanaRpc, mint: &SdkPubkey) -> Result<u8> {
    if let Ok(supply) = rpc.rpc.get_token_supply(mint).await {
        MINT_DECIMALS_SOURCE_SUPPLY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return Ok(supply.decimals);
    }
    let acct = rpc.rpc.get_account(mint).await?;
    if acct.data.len() > 44 {
        MINT_DECIMALS_SOURCE_ACCOUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(acct.data[44])
    } else {
        Err(anyhow!("mint account data too short to read decimals"))
    }
}
