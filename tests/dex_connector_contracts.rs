//! Phase 3 Contract Tests: DEX Connector Invariants (DoD §H)
//!
//! Verifies mathematical and behavioral invariants for DEX connectors.
//! PumpSwap AMM: quote monotonicity, price impact, unknown pair, zero input, build IX validity.

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
            sell_cashback_remaining: false,
            sell_cashback_third_meta: None,
        }),
        100,
    );
    Arc::new(cache)
}

fn make_empty_cache() -> Arc<LivePoolCache> {
    Arc::new(LivePoolCache::new())
}

// =============================================================================
// PumpSwap AMM Contract Tests
// =============================================================================

#[tokio::test]
async fn contract_pump_amm_quote_monotonic() {
    let base_mint = Pubkey::new_unique();
    let pool_market = Pubkey::new_unique();
    let cache = make_pump_amm_cache_with_reserves(
        pool_market,
        base_mint,
        1_000_000_000_000,
        50_000_000_000,
    );
    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
    let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

    let base_mint_str = base_mint.to_string();

    let out1 = dex
        .quote_exact_in(WSOL_MINT, &base_mint_str, 100_000)
        .await
        .expect("quote ok")
        .map(|q| q.amount_out);
    let out2 = dex
        .quote_exact_in(WSOL_MINT, &base_mint_str, 1_000_000)
        .await
        .expect("quote ok")
        .map(|q| q.amount_out);
    let out3 = dex
        .quote_exact_in(WSOL_MINT, &base_mint_str, 10_000_000)
        .await
        .expect("quote ok")
        .map(|q| q.amount_out);

    assert!(out1.is_some() && out1.unwrap() > 0);
    assert!(out2.is_some() && out2.unwrap() > 0);
    assert!(out3.is_some() && out3.unwrap() > 0);
    assert!(out1.unwrap() <= out2.unwrap());
    assert!(out2.unwrap() <= out3.unwrap());
}

#[tokio::test]
async fn contract_pump_amm_price_impact_non_decreasing() {
    let base_mint = Pubkey::new_unique();
    let pool_market = Pubkey::new_unique();
    let cache = make_pump_amm_cache_with_reserves(
        pool_market,
        base_mint,
        1_000_000_000_000,
        50_000_000_000,
    );
    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
    let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

    let base_mint_str = base_mint.to_string();

    let impact1 = dex
        .quote_exact_in(WSOL_MINT, &base_mint_str, 1_000_000)
        .await
        .expect("quote ok")
        .map(|q| q.price_impact_bps);
    let impact2 = dex
        .quote_exact_in(WSOL_MINT, &base_mint_str, 10_000_000)
        .await
        .expect("quote ok")
        .map(|q| q.price_impact_bps);
    let impact3 = dex
        .quote_exact_in(WSOL_MINT, &base_mint_str, 100_000_000)
        .await
        .expect("quote ok")
        .map(|q| q.price_impact_bps);

    assert!(impact1.is_some());
    assert!(impact2.is_some());
    assert!(impact3.is_some());
    assert!(impact1.unwrap() <= impact2.unwrap());
    assert!(impact2.unwrap() <= impact3.unwrap());
}

#[tokio::test]
async fn contract_pump_amm_unknown_pair_returns_none() {
    let base_mint = Pubkey::new_unique();
    let cache = make_empty_cache();
    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
    let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

    let base_mint_str = base_mint.to_string();
    let result = dex
        .quote_exact_in(WSOL_MINT, &base_mint_str, 1_000_000)
        .await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn contract_pump_amm_zero_input() {
    let base_mint = Pubkey::new_unique();
    let pool_market = Pubkey::new_unique();
    let cache = make_pump_amm_cache_with_reserves(
        pool_market,
        base_mint,
        1_000_000_000_000,
        50_000_000_000,
    );
    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
    let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

    let base_mint_str = base_mint.to_string();
    let result = dex.quote_exact_in(WSOL_MINT, &base_mint_str, 0).await;

    assert!(result.is_ok());
    let quote_opt = result.unwrap();
    assert!(
        quote_opt.is_none() || quote_opt.as_ref().unwrap().amount_out == 0,
        "zero input must yield None or amount_out == 0"
    );
}

#[test]
fn contract_pump_amm_build_ix_valid_accounts() {
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
        false,
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
