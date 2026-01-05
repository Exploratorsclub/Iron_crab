#![cfg(all(feature = "test_helpers", feature = "legacy_sniper"))]
use ironcrab::metrics::{
    record_trade_return, reset_trade_return_metrics, TRADE_RETURN_BUCKET_COUNTS,
    TRADE_RETURN_COUNT, TRADE_RETURN_SUM_MICRO,
};
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::sniper::{SniperCfg, SniperEngine};
use ironcrab::wallet::Treasury;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn drawdown_sizing_scales_max_buy() {
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    let mut cfg = SniperCfg::default();
    cfg.max_buy_sol = 10.0;
    cfg.daily_loss_limit_sol = Some(100.0);
    cfg.drawdown_scale_start = Some(0.3); // start reducing at 30% of limit
    cfg.drawdown_max_reduction = Some(0.6); // up to 60% reduction at 100% drawdown => min 40% of base
    let tmp = std::env::temp_dir().join("ironcrab_test_key_drawdown.json");
    if !tmp.exists() {
        let kp = solana_sdk::signature::Keypair::new();
        std::fs::write(&tmp, serde_json::to_vec(&kp.to_bytes().to_vec()).unwrap()).unwrap();
    }
    let treasury = Arc::new(Treasury::load(tmp.to_str().unwrap()).unwrap());
    let engine = SniperEngine::new(rpc, cfg.clone(), None, None, None, treasury, None, None);

    // At 0% drawdown -> full size
    engine.test_set_realized_loss_today(0.0);
    assert!((engine.test_effective_max_buy_sol() - 10.0).abs() < 1e-9);

    // At 30% drawdown -> still full size (threshold)
    engine.test_set_realized_loss_today(30.0);
    assert!((engine.test_effective_max_buy_sol() - 10.0).abs() < 1e-9);

    // At 65% drawdown -> between start and max
    engine.test_set_realized_loss_today(65.0);
    let eff = engine.test_effective_max_buy_sol();
    assert!(eff < 10.0 && eff > 4.0, "eff={}", eff); // min would be 4.0 at 100%

    // At 100% drawdown -> max reduction applied: 40% of 10 = 4 SOL
    engine.test_set_realized_loss_today(100.0);
    assert!((engine.test_effective_max_buy_sol() - 4.0).abs() < 1e-9);
}

#[tokio::test]
async fn cooldown_gating_blocks_and_expires() {
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    let mut cfg = SniperCfg::default();
    cfg.stop_loss_cooldown_secs = Some(1); // short cooldown
    let tmp = std::env::temp_dir().join("ironcrab_test_key_cooldown.json");
    if !tmp.exists() {
        let kp = solana_sdk::signature::Keypair::new();
        std::fs::write(&tmp, serde_json::to_vec(&kp.to_bytes().to_vec()).unwrap()).unwrap();
    }
    let treasury = Arc::new(Treasury::load(tmp.to_str().unwrap()).unwrap());
    let engine = SniperEngine::new(rpc, cfg.clone(), None, None, None, treasury, None, None);

    let mint = Pubkey::new_unique();
    // Mark cooldown now
    engine.test_mark_cooldown(mint);
    // Should block open immediately
    assert!(!engine.test_can_open_position_for(&mint, 0.5));
    // Wait for expiry
    tokio::time::sleep(Duration::from_millis(1100)).await;
    // Should allow now
    assert!(engine.test_can_open_position_for(&mint, 0.5));

    // Also test manual set in the future
    let future = chrono::Utc::now().timestamp() + 5;
    engine.test_set_cooldown(mint, future);
    assert!(!engine.test_can_open_position_for(&mint, 0.5));
}

#[tokio::test]
async fn trade_return_bucketing_records_counts_and_sum() {
    reset_trade_return_metrics();
    // Place some returns covering negative, zero, and positive buckets
    let rets = [-0.6, -0.03, 0.0, 0.015, 0.5, 3.0];
    for r in rets {
        record_trade_return(r);
    }

    // Count should equal number of samples
    assert_eq!(
        TRADE_RETURN_COUNT.load(std::sync::atomic::Ordering::Relaxed),
        6
    );
    // Sum micro should reflect sum(rets)
    let sum = rets.iter().sum::<f64>();
    let micro = TRADE_RETURN_SUM_MICRO.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(micro, (sum * 1_000_000.0).round() as i64);

    // Check that at least some bucket counters moved (not all zero)
    let mut any = false;
    for c in TRADE_RETURN_BUCKET_COUNTS.iter() {
        if c.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            any = true;
            break;
        }
    }
    assert!(any, "expected at least one bucket to be incremented");
}
