#![cfg(feature = "test_helpers")]
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::sniper::{SniperCfg, SniperEngine};
use ironcrab::wallet::Treasury;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

fn temp_treasury(label: &str) -> Arc<Treasury> {
    let path = std::env::temp_dir().join(format!("ironcrab_{label}_kp.json"));
    if !path.exists() {
        let kp = solana_sdk::signature::Keypair::new();
        std::fs::write(&path, serde_json::to_vec(&kp.to_bytes().to_vec()).unwrap()).unwrap();
    }
    Arc::new(Treasury::load(path.to_str().unwrap()).unwrap())
}

#[tokio::test]
async fn sharpe_lower_with_fees() {
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    let base_cfg = SniperCfg {
        rolling_pnl_window: Some(20),
        ..SniperCfg::default()
    };
    let eng_no_fee = SniperEngine::new(
        rpc.clone(),
        base_cfg.clone(),
        None,
        None,
        temp_treasury("fee0"),
    );
    let eng_fee = SniperEngine::new(rpc.clone(), base_cfg, None, None, temp_treasury("fee1"));

    let mint_a = Pubkey::new_unique();
    let mint_b = Pubkey::new_unique();
    eng_no_fee.test_insert_lot(mint_a, 10.0, 1_000_000.0, 0.00001, 6);
    eng_fee.test_insert_lot(mint_b, 10.0, 1_000_000.0, 0.00001, 6);

    let fractions = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1];
    let gross_returns = [0.05, -0.02, 0.04, 0.03, -0.01, 0.06];

    for (f, r) in fractions.into_iter().zip(gross_returns.into_iter()) {
        // No-fee engine
        let slice_no_fee = eng_no_fee
            .test_current_invested_for_lot(&mint_a, 0)
            .unwrap()
            * f;
        let proceeds_no_fee = slice_no_fee * (1.0 + r);
        let _ = eng_no_fee
            .test_simulate_partial_exit_with_fee(&mint_a, 0, f, proceeds_no_fee, 0.0)
            .unwrap();

        // Fee engine: apply fee only on positive returns (50% of profit)
        let slice_fee = eng_fee.test_current_invested_for_lot(&mint_b, 0).unwrap() * f;
        let (proceeds_fee, fee_sol) = if r > 0.0 {
            let profit = slice_fee * r;
            let fee = profit * 0.5; // take half of profit as fee
            (slice_fee + profit, fee)
        } else {
            (slice_fee * (1.0 + r), 0.0)
        };
        let _ = eng_fee
            .test_simulate_partial_exit_with_fee(&mint_b, 0, f, proceeds_fee, fee_sol)
            .unwrap();
    }

    let (sharpe_no_fee, c_no_fee) = eng_no_fee.test_get_sharpe();
    let (sharpe_fee, c_fee) = eng_fee.test_get_sharpe();
    assert_eq!(c_no_fee, c_fee, "sample counts should match");
    assert!(sharpe_no_fee.is_finite() && sharpe_fee.is_finite());
    assert!(
        sharpe_fee < sharpe_no_fee,
        "Sharpe with fees should be lower ({} !< {})",
        sharpe_fee,
        sharpe_no_fee
    );
}

#[tokio::test]
async fn rolling_window_truncation() {
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    let cfg = SniperCfg {
        rolling_pnl_window: Some(5),
        ..SniperCfg::default()
    };
    let engine = SniperEngine::new(rpc, cfg, None, None, temp_treasury("window"));
    let mint = Pubkey::new_unique();
    engine.test_insert_lot(mint, 12.0, 2_000_000.0, 0.00002, 6);

    // Perform 8 partial exits -> more than window size
    for _ in 0..8 {
        let invest_slice = engine.test_current_invested_for_lot(&mint, 0).unwrap() * 0.1; // dynamic per remaining
        let r = 0.02; // +2%
        let proceeds = invest_slice * (1.0 + r);
        let _ = engine
            .test_simulate_partial_exit_with_fee(&mint, 0, 0.1, proceeds, 0.0)
            .unwrap();
        let (_sharpe, count) = engine.test_get_sharpe();
        assert!(
            count <= 5,
            "recent_realized should not exceed window, got {count}"
        );
    }
    let (_final_sharpe, final_count) = engine.test_get_sharpe();
    assert_eq!(final_count, 5, "window truncation should cap at 5 entries");
}
