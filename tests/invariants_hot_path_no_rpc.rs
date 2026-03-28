//! Invariante: Hot-Path RPC-Freiheit (INVARIANTS.md A.12, I-4, I-7)
//!
//! DEX-Connectors mit allow_rpc_on_miss=false (bzw. live_pool_cache bei Orca) liefern bei
//! Cache-Miss None/Err ohne RPC. Hot Path (Arb, Momentum) darf keine blockierenden RPC-Calls.

use ironcrab::execution::live_pool_cache::LivePoolCache;
use ironcrab::solana::dex::meteora_dlmm::MeteoraDlmm;
use ironcrab::solana::dex::orca::Orca;
use ironcrab::solana::dex::pumpfun_amm::PumpFunAmmDex;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::dex::raydium_cpmm::RaydiumCpmm;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const DUMMY_RPC: &str = "http://127.0.0.1:0";

/// PumpFunAmmDex: Cache-Miss bei quote_exact_in → Ok(None), kein RPC
#[tokio::test]
async fn pump_amm_quote_cache_miss_no_rpc() {
    let cache = Arc::new(LivePoolCache::new());
    let rpc = Arc::new(SolanaRpc::new(DUMMY_RPC));
    let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

    let unknown_mint = Pubkey::new_unique();
    let result = dex
        .quote_exact_in(WSOL_MINT, &unknown_mint.to_string(), 1_000_000)
        .await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

/// PumpFunAmmDex: Cache-Miss bei pool_accounts_v1_for_base_mint → Ok(None), kein RPC
#[tokio::test]
async fn pump_amm_pool_accounts_cache_miss_no_rpc() {
    let cache = Arc::new(LivePoolCache::new());
    let rpc = Arc::new(SolanaRpc::new(DUMMY_RPC));
    let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

    let unknown_mint = Pubkey::new_unique();
    let result = dex.pool_accounts_v1_for_base_mint(unknown_mint).await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

/// Raydium: Vault-Cache-Miss bei fetch_and_update_reserves → Err mit GEYSER-ONLY
#[tokio::test]
async fn raydium_vault_cache_miss_no_rpc() {
    let rpc = Arc::new(SolanaRpc::new(DUMMY_RPC));
    let raydium = Raydium::new_with_live_cache(rpc, None, false);

    let pool_addr = Pubkey::new_unique();
    let base_mint = Pubkey::new_unique();
    let quote_mint = Pubkey::new_unique();
    let base_vault = Pubkey::new_unique();
    let quote_vault = Pubkey::new_unique();
    let market_id = Pubkey::new_unique();

    raydium.inject_cached_amm_state(
        pool_addr,
        base_mint,
        quote_mint,
        base_vault,
        quote_vault,
        9,
        6,
        None,
        None,
        market_id,
        None,
        None,
        None,
    );

    let result = raydium.fetch_and_update_reserves(&pool_addr).await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("GEYSER-ONLY") || err_msg.contains("LivePoolCache"),
        "expected GEYSER-ONLY or LivePoolCache in error, got: {}",
        err_msg
    );
}

/// RaydiumCpmm: Vault-Cache-Miss bei quote_exact_in → Err mit GEYSER-ONLY
#[tokio::test]
async fn raydium_cpmm_quote_cache_miss_no_rpc() {
    let rpc = Arc::new(SolanaRpc::new(DUMMY_RPC));
    let cpmm = RaydiumCpmm::new_with_live_cache(rpc, None, false);

    let pool_addr = Pubkey::new_unique();
    let token_0 = Pubkey::new_unique();
    let token_1 = Pubkey::new_unique();
    let vault_0 = Pubkey::new_unique();
    let vault_1 = Pubkey::new_unique();

    cpmm.set_pool_from_accounts(
        &pool_addr.to_string(),
        &[
            pool_addr.to_string(),
            token_0.to_string(),
            token_1.to_string(),
            vault_0.to_string(),
            vault_1.to_string(),
        ],
    )
    .expect("set_pool_from_accounts");

    let result = cpmm
        .quote_exact_in(&token_0.to_string(), &token_1.to_string(), 1_000_000)
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("GEYSER-ONLY"),
        "expected GEYSER-ONLY in error, got: {}",
        err_msg
    );
}

/// MeteoraDlmm: Vault-Cache-Miss bei quote_exact_in → Err mit GEYSER-ONLY
#[tokio::test]
async fn meteora_dlmm_quote_cache_miss_no_rpc() {
    let rpc = Arc::new(SolanaRpc::new(DUMMY_RPC));
    let meteora = MeteoraDlmm::new_with_live_cache(rpc, None, false);

    let pool_addr = Pubkey::new_unique();
    let token_x = Pubkey::new_unique();
    let token_y = Pubkey::new_unique();
    let reserve_x = Pubkey::new_unique();
    let reserve_y = Pubkey::new_unique();

    meteora
        .set_pool_from_accounts(
            &pool_addr.to_string(),
            &[
                pool_addr.to_string(),
                token_x.to_string(),
                token_y.to_string(),
                reserve_x.to_string(),
                reserve_y.to_string(),
            ],
        )
        .expect("set_pool_from_accounts");

    let result = meteora
        .quote_exact_in(&token_x.to_string(), &token_y.to_string(), 1_000_000)
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("GEYSER-ONLY"),
        "expected GEYSER-ONLY in error, got: {}",
        err_msg
    );
}

/// Orca: live_pool_cache gesetzt, Vault-Cache-Miss → Ok(None), kein RPC
#[tokio::test]
async fn orca_quote_cache_miss_no_rpc() {
    let rpc = Arc::new(SolanaRpc::new(DUMMY_RPC));
    let live_pool_cache = Arc::new(LivePoolCache::new());
    let orca = Orca::new_with_cache(rpc, None, Some(live_pool_cache));

    let base_mint = Pubkey::new_unique();
    let quote_mint = Pubkey::from_str(WSOL_MINT).unwrap();
    orca.insert_mock_pool(base_mint, quote_mint, 0, 0, 30);

    let result = orca
        .quote_exact_in(&base_mint.to_string(), &quote_mint.to_string(), 1_000_000)
        .await;

    assert!(result.is_ok());
    let quote = result.unwrap();
    assert!(quote.is_none());
}
