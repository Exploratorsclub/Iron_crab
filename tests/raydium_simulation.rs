use std::sync::Arc;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::dex::Dex; // trait

// NOTE: This test performs live RPC calls (devnet / mainnet) depending on SOLANA_RPC_URL env.
// It is marked ignore by default to avoid CI flakiness.
#[tokio::test]
#[ignore]
async fn raydium_swap_plan_simulation_layout() {
    let url = std::env::var("SOLANA_RPC_URL").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
    let rpc = Arc::new(SolanaRpc::new(&url));
    let raydium = Raydium::new(rpc.clone());
    // Refresh pools (may be large; for layout test only). If fails, skip.
    if let Err(e) = raydium.refresh_pools().await { eprintln!("skip: refresh failed {e}"); return; }
    let snaps = raydium.snapshots();
    if snaps.is_empty() { eprintln!("skip: no pools fetched"); return; }
    // Pick first snapshot for a synthetic plan: we only verify compute budget IX ordering & data structure.
    let pool = &snaps[0];
    // Build a plan with compute budget instructions (limit+price) using base->quote path (amount arbitrary)
    let amount_in = 1_000u64;
    let plan_opt = raydium.build_swap_plan(&pool.base_mint.to_string(), &pool.quote_mint.to_string(), amount_in, 50, Some(500_000), Some(5_000)).expect("plan build result");
    if plan_opt.is_none() { eprintln!("skip: no plan for chosen pool"); return; }
    let plan = plan_opt.unwrap();
    // Assert first 1-2 instructions are compute budget (order can be: set CU limit, then price)
    // Compute budget Ixs present when requested
    assert!(plan.ixs.len() >= 2, "expected compute budget + swap ix");
    let cb_prog = ironcrab::solana::compute_budget_helper::program_id();
    assert_eq!(plan.ixs[0].program_id, cb_prog);
    // Basic min_out correctness (slippage 50 bps)
    let expected_min = (plan.expected_out as u128 * (10_000 - 50) as u128 / 10_000) as u64;
    assert_eq!(plan.min_out, expected_min, "min_out slippage calc mismatch");
}
