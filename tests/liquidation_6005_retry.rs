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

#[test]
fn retry_not_triggered_when_dex_is_pump_amm() {
    let intent = make_intent("pump_amm");
    let err = anyhow::anyhow!("Simulation failed: InstructionError(1, Custom(6005))");
    assert!(!would_retry_6005(&err, &intent));
}
