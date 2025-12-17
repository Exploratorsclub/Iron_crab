#![cfg(feature = "test_helpers")]
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::sniper::{SniperCfg, SniperEngine};
use ironcrab::wallet::Treasury;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

// Integration-like flow using only in-memory state + test helpers
// 1) Insert a buy (open lot)
// 2) Simulate a partial fill exit (realized loss/profit)
// 3) Simulate final exit (lot removed), verify realized PnL and position state
#[tokio::test]
async fn mock_buy_fill_sell_lifecycle() {
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    let cfg = SniperCfg {
        max_buy_sol: 2.0,
        ..SniperCfg::default()
    };
    let tmp = std::env::temp_dir().join("ironcrab_test_key_integration.json");
    if !tmp.exists() {
        let kp = solana_sdk::signature::Keypair::new();
        std::fs::write(&tmp, serde_json::to_vec(&kp.to_bytes().to_vec()).unwrap()).unwrap();
    }
    let treasury = Arc::new(Treasury::load(tmp.to_str().unwrap()).unwrap());
    let engine = SniperEngine::new(rpc, cfg, None, None, None, treasury, None);

    let mint = Pubkey::new_unique();

    // BUY: open a position lot (simulating a purchase of tokens worth 1 SOL)
    engine.test_insert_lot(mint, 1.0, 1000.0, 0.001, 6);
    assert_eq!(engine.test_open_lot_count(&mint), 1);
    assert_eq!(engine.test_total_open_positions(), 1);

    // FILL 1: realize a partial profit of +10% on 20% of position
    let (r1, _sh, n1) = engine
        .test_simulate_partial_exit_with_fee(&mint, 0, 0.2, 0.22, 0.0)
        .expect("partial exit 1");
    assert!((r1 - 0.10).abs() < 1e-9);
    assert!(n1 >= 1);
    // Invested on lot should reduce to 0.8 SOL, tokens to 800
    assert!((engine.test_current_invested_for_lot(&mint, 0).unwrap() - 0.8).abs() < 1e-9);

    // FILL 2: realize a loss of -5% on next 50% of remaining (i.e., 40% of original)
    let (_r2, _sh2, _n2) = engine
        .test_simulate_partial_exit_with_fee(&mint, 0, 0.5, 0.76 * 0.5, 0.0) // invest_slice was 0.8*0.5=0.4; proceeds 0.38 => -5%
        .expect("partial exit 2");
    // Remaining invested should now be 0.4 SOL
    assert!((engine.test_current_invested_for_lot(&mint, 0).unwrap() - 0.4).abs() < 1e-9);

    // SELL ALL: final exit at break-even for remaining 0.4 SOL slice
    let (_r3, _sh3, _n3) = engine
        .test_simulate_partial_exit_with_fee(&mint, 0, 1.0, 0.4, 0.0)
        .expect("final exit");
    // Position removed
    assert_eq!(engine.test_open_lot_count(&mint), 0);
    assert_eq!(engine.test_total_open_positions(), 0);

    // Verify realized PnL roughly equals: +0.02 (first) + (-0.02) (second) + 0.0 (final) = 0.0
    let realized = engine.test_get_realized_pnl_sol();
    assert!(realized.abs() < 1e-9, "realized={}", realized);
}
