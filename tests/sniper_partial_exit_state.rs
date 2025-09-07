#![cfg(feature = "test_helpers")]
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::sniper::{SniperCfg, SniperEngine};
use ironcrab::wallet::Treasury;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

#[tokio::test]
async fn partial_exit_state_reduction() {
    // Build barebones engine (real network calls avoided by not invoking attempt_exit itself)
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    let mut cfg = SniperCfg::default();
    cfg.max_slippage_bps = 50;
    let raydium: Option<Arc<ironcrab::solana::dex::raydium::Raydium>> = None; // skip DEX
    let orca: Option<Arc<ironcrab::solana::dex::orca::Orca>> = None;
    // Load a dummy keypair from an in-memory temp file: create ephemeral keypair and write to temp path.
    let tmp_path = {
        let dir = std::env::temp_dir();
        let path = dir.join("ironcrab_test_key.json");
        if !path.exists() {
            let kp = solana_sdk::signature::Keypair::new();
            std::fs::write(&path, serde_json::to_vec(&kp.to_bytes().to_vec()).unwrap()).unwrap();
        }
        path
    };
    let treasury = Arc::new(Treasury::load(tmp_path.to_str().unwrap()).unwrap());
    let engine = SniperEngine::new(rpc, cfg, raydium, orca, treasury);

    let mint = Pubkey::new_unique();
    let invested_sol = 8.0;
    let amount_tokens = 2_000_000.0;
    let entry_price = 0.000004; // arbitrary

    engine.test_insert_lot(mint, invested_sol, amount_tokens, entry_price, 6);

    // Simulate partial exit of 40%
    let fraction = 0.4;
    let realized_delta = invested_sol * fraction * 0.10; // +10% on sold slice
    let res = engine
        .test_apply_partial_reduction(&mint, 0, fraction, realized_delta)
        .expect("lot exists");
    let (remaining_invested, remaining_tokens, realized_added, lots_remaining) = res;

    assert_eq!(lots_remaining, 1, "lot should remain after partial exit");
    assert!(
        (remaining_invested - invested_sol * (1.0 - fraction)).abs() < 1e-9,
        "invested capital not reduced proportionally"
    );
    assert!(
        (remaining_tokens - amount_tokens * (1.0 - fraction)).abs() < 1e-3,
        "token amount not reduced proportionally"
    );
    assert!(
        (realized_added - realized_delta).abs() < 1e-12,
        "realized delta not applied"
    );

    // Full exit of remaining lot
    let res2 = engine
        .test_apply_partial_reduction(&mint, 0, 1.0, remaining_invested)
        .expect("second reduction");
    let (_inv2, _tok2, _real2, lots_remaining2) = res2;
    assert_eq!(lots_remaining2, 0, "lot should be removed after full exit");
}
