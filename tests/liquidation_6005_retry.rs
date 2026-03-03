//! Integration/Manual Test: 6005-Retry-Mechanismus in Liquidation (FIX-44)
//!
//! Verifiziert die Retry-Entscheidungslogik: Bei Sim-Fail mit 6005 (BondingCurveComplete)
//! und dex=pumpfun soll automatisch mit PumpSwap AMM retried werden.
//!
//! Unit-Tests für is_6005_bonding_curve_complete liegen in ironcrab-eval.
//! Dieser Test prüft die kombinierte Retry-Bedingung.
//!
//! Manueller Ablauf (Devnet/Mainnet):
//! 1. Kill-Switch aktivieren mit Liquidation
//! 2. Wallet mit Token das eine migrierte PumpFun-BC hat (6005)
//! 3. Erwartung: Erst PumpFun-Versuch → Sim-Fail 6005 → Retry mit pump_amm → Erfolg

use ironcrab::execution::error_detection::is_6005_bonding_curve_complete;
use ironcrab::ipc::{IntentOrigin, IntentTier, TradeIntent, TradingRegime};
use uuid::Uuid;

fn make_intent(dex: &str) -> TradeIntent {
    let run_id = Uuid::new_v4().to_string();
    let mut intent = TradeIntent::new_sell(
        "test",
        "0.0.0",
        &run_id,
        format!("test-{}", Uuid::new_v4()),
        "test",
        IntentTier::Tier0,
        IntentOrigin::ExecutionMevB,
        "TokenMint11111111111111111111111111111111".to_string(),
        6,
        "So11111111111111111111111111111111111111112".to_string(),
        1_000_000,
        0,
        500,
        TradingRegime::NotApplicable,
    );
    intent.metadata.insert("dex".to_string(), dex.to_string());
    intent
        .metadata
        .insert("purpose".to_string(), "liquidation".to_string());
    intent
}

/// Retry-Bedingung wie in run_liquidation_job: 6005 + dex=pumpfun
fn would_retry_6005(err: &impl std::fmt::Display, intent: &TradeIntent) -> bool {
    is_6005_bonding_curve_complete(err)
        && intent.metadata.get("dex").map(|s| s.as_str()) == Some("pumpfun")
}

#[test]
fn retry_triggered_on_6005_and_pumpfun() {
    let intent = make_intent("pumpfun");
    let err = anyhow::anyhow!("Simulation failed: InstructionError(1, Custom(6005))");
    assert!(would_retry_6005(&err, &intent));
}

#[test]
fn retry_not_triggered_on_6023() {
    let intent = make_intent("pumpfun");
    let err = anyhow::anyhow!("Simulation failed: Custom(6023)");
    assert!(!would_retry_6005(&err, &intent));
}

/// Verifiziert, dass die 6005-Fixture gültige Base58-Pubkeys hat und der Cache-Lookup funktioniert.
#[test]
fn fixture_pubkeys_parse_and_cache_hit() {
    use ironcrab::execution::live_pool_cache::{CachedPoolState, LivePoolCache, PumpAmmState};
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    let fixture_json = include_str!("fixtures/golden_replays/liquidation_6005_retry_fixture.json");
    let fixture: serde_json::Value = serde_json::from_str(fixture_json).unwrap();
    let base_mint_str = fixture["base_mint"].as_str().unwrap();
    let pool_market_str = fixture["pool_market"].as_str().unwrap();
    let quote_mint_str = fixture["quote_mint"].as_str().unwrap();
    let pool_accounts_arr = fixture["pool_accounts"].as_array().unwrap();

    let base_mint = Pubkey::from_str(base_mint_str).expect("base_mint must parse");
    let pool_market = Pubkey::from_str(pool_market_str).expect("pool_market must parse");
    let quote_mint = Pubkey::from_str(quote_mint_str).expect("quote_mint must parse");
    let mut pool_accounts = Vec::new();
    for (i, v) in pool_accounts_arr.iter().enumerate() {
        let s = v.as_str().unwrap();
        match Pubkey::from_str(s) {
            Ok(p) => pool_accounts.push(p),
            Err(e) => panic!("pool_accounts[{}] '{}' parse error: {}", i, s, e),
        }
    }
    assert_eq!(pool_accounts.len(), 14, "need 14 pool_accounts for PumpAmm");
    assert!(base_mint_str.len() >= 43, "base_mint should be 43–44 chars");

    let cache = LivePoolCache::new();
    let (pool_base, pool_quote) = (pool_accounts[4], pool_accounts[5]);
    cache.upsert(
        pool_market,
        CachedPoolState::PumpAmm(PumpAmmState {
            base_mint,
            quote_mint,
            pool_base_token_account: pool_base,
            pool_quote_token_account: pool_quote,
            base_reserve: Some(1_000_000_000_000),
            quote_reserve: Some(50_000_000_000),
            pool_accounts: pool_accounts.clone(),
            creator: None,
        }),
        0,
    );

    let reserves = cache.get_pump_amm_reserves_by_base_mint(&base_mint);
    assert!(
        reserves.is_some(),
        "get_pump_amm_reserves_by_base_mint should hit"
    );
    let accounts = cache.get_pump_amm_pool_accounts_by_base_mint(&base_mint);
    assert!(
        accounts.is_some(),
        "get_pump_amm_pool_accounts_by_base_mint should hit"
    );
    assert_eq!(accounts.unwrap().len(), 14);
}

#[test]
fn retry_not_triggered_when_dex_is_pump_amm() {
    let intent = make_intent("pump_amm");
    let err = anyhow::anyhow!("Simulation failed: InstructionError(1, Custom(6005))");
    assert!(!would_retry_6005(&err, &intent));
}
