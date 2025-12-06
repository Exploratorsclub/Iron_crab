#![cfg(feature = "test_helpers")]
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::sniper::{SniperCfg, SniperEngine};
use ironcrab::wallet::Treasury;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

#[tokio::test]
async fn sharpe_updates_after_partial_exits() {
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    let cfg = SniperCfg {
        rolling_pnl_window: Some(20),
        ..SniperCfg::default()
    };
    // temp keypair file
    let tmp_path = {
        let dir = std::env::temp_dir();
        let path = dir.join("ironcrab_test_key_sharpe.json");
        if !path.exists() {
            let kp = solana_sdk::signature::Keypair::new();
            std::fs::write(&path, serde_json::to_vec(&kp.to_bytes().to_vec()).unwrap()).unwrap();
        }
        path
    };
    let treasury = Arc::new(Treasury::load(tmp_path.to_str().unwrap()).unwrap());
    let engine = SniperEngine::new(rpc, cfg, None, None, treasury, None);

    let mint = Pubkey::new_unique();
    engine.test_insert_lot(mint, 10.0, 1_000_000.0, 0.00001, 6);

    // Simulate 6 partial exits with different returns to exceed Sharpe threshold (>=5 samples)
    let fractions = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1]; // total 60% exited over time
    let returns = [0.05, -0.02, 0.10, 0.03, -0.01, 0.08]; // per-slice returns

    for (f, r) in fractions.into_iter().zip(returns.into_iter()) {
        let current_invest_slice = engine.test_current_invested_for_lot(&mint, 0).unwrap() * f;
        let proceeds = current_invest_slice * (1.0 + r);
        let fee = 0.0;
        let _ = engine
            .test_simulate_partial_exit_with_fee(&mint, 0, f, proceeds, fee)
            .expect("simulation");
    }

    let (sharpe, count) = engine.test_get_sharpe();
    assert!(
        count >= 5,
        "need at least 5 samples for Sharpe, got {count}"
    );
    assert!(
        sharpe.is_finite() && sharpe.abs() > 0.0,
        "Sharpe should be computed and non-zero, got {sharpe}"
    );
}
