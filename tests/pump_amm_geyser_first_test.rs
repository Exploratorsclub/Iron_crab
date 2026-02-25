//! Phase 2 Integration Tests: PumpFunAmmDex + LivePoolCache Roundtrip
//!
//! Verifies A.1 Geyser-First: With LivePoolCache and cache hit, no RPC is called.
//! RPC uses unreachable URL (http://127.0.0.1:0) — tests pass only if cache path is used.

use ironcrab::execution::live_pool_cache::{CachedPoolState, LivePoolCache, PumpAmmState};
use ironcrab::solana::dex::pumpfun_amm::PumpFunAmmDex;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const PUMPFUN_AMM_PROGRAM_ID: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

fn make_pump_amm_cache_with_reserves(
    pool_market: Pubkey,
    base_mint: Pubkey,
    base_reserve: u64,
    quote_reserve: u64,
) -> Arc<LivePoolCache> {
    let cache = LivePoolCache::new();
    cache.upsert(
        pool_market,
        CachedPoolState::PumpAmm(PumpAmmState {
            base_mint,
            quote_mint: Pubkey::from_str(WSOL_MINT).unwrap(),
            pool_base_token_account: Pubkey::new_unique(),
            pool_quote_token_account: Pubkey::new_unique(),
            base_reserve: Some(base_reserve),
            quote_reserve: Some(quote_reserve),
            pool_accounts: vec![],
            creator: None,
        }),
        100,
    );
    Arc::new(cache)
}

fn make_pump_amm_cache_with_pool_accounts(
    pool_market: Pubkey,
    base_mint: Pubkey,
    pool_accounts: Vec<Pubkey>,
) -> Arc<LivePoolCache> {
    let cache = LivePoolCache::new();
    cache.upsert(
        pool_market,
        CachedPoolState::PumpAmm(PumpAmmState {
            base_mint,
            quote_mint: Pubkey::from_str(WSOL_MINT).unwrap(),
            pool_base_token_account: Pubkey::new_unique(),
            pool_quote_token_account: Pubkey::new_unique(),
            base_reserve: Some(1),
            quote_reserve: Some(1),
            pool_accounts,
            creator: None,
        }),
        100,
    );
    Arc::new(cache)
}

#[tokio::test]
async fn test_quote_from_cache_no_rpc() {
    let base_mint = Pubkey::new_unique();
    let pool_market = Pubkey::new_unique();
    let cache = make_pump_amm_cache_with_reserves(
        pool_market,
        base_mint,
        1_000_000_000_000,
        50_000_000_000,
    );
    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
    let dex = PumpFunAmmDex::new_with_cache(rpc, cache);

    let base_mint_str = base_mint.to_string();
    let result = dex
        .quote_exact_in(WSOL_MINT, &base_mint_str, 1_000_000_000)
        .await;

    let quote = result.expect("quote should succeed");
    assert!(quote.is_some(), "expected Some(Quote) on cache hit");
    let quote = quote.unwrap();
    assert!(quote.amount_out > 0);
    assert!(
        quote.price_impact_bps < 10_000,
        "price_impact_bps should be plausible"
    );
    assert!(quote.route.contains(&pool_market.to_string()));
    assert_eq!(quote.fee_bps, 125);
}

#[tokio::test]
async fn test_pool_accounts_from_cache_no_rpc() {
    let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
    let base_mint = Pubkey::new_unique();
    let pool_market = Pubkey::new_unique();
    let pool_accounts: Vec<Pubkey> = vec![
        pool_market,
        Pubkey::new_unique(),
        base_mint,
        wsol,
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
    ];
    assert_eq!(pool_accounts.len(), 14);

    let cache =
        make_pump_amm_cache_with_pool_accounts(pool_market, base_mint, pool_accounts.clone());
    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
    let dex = PumpFunAmmDex::new_with_cache(rpc, cache);

    let result = dex.pool_accounts_v1_for_base_mint(base_mint).await;

    assert!(result.is_ok());
    let accounts = result.unwrap();
    assert!(accounts.is_some());
    let accounts = accounts.unwrap();
    assert_eq!(accounts.len(), 14);
    assert_eq!(accounts, pool_accounts);
}

#[test]
fn test_build_swap_ix_with_cached_accounts() {
    let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
    let base_mint = Pubkey::new_unique();
    let base_mint_str = base_mint.to_string();
    let user = Pubkey::new_unique();

    let pool_accounts: Vec<Pubkey> = vec![
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        base_mint,
        wsol,
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
    ];
    assert_eq!(pool_accounts.len(), 14);

    let result = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
        WSOL_MINT,
        &base_mint_str,
        1_000_000_000,
        100_000,
        user,
        &pool_accounts,
        None,
    );

    assert!(result.is_ok());
    let ixs = result.unwrap();
    assert!(!ixs.is_empty());
    assert_eq!(
        ixs[0].program_id,
        Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).unwrap()
    );
    assert!(!ixs[0].data.is_empty());
}
