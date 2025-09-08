use ironcrab::solana::dex::{orca::Orca, router::Router, Dex};
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

// Integration-style test: Stub signals -> Router -> Swap plan assembly (2 hops)
// Builds two Orca connectors with deterministic mock pools A-B and B-C, then asks the Router
// to find the best 2-hop route A->B->C and assemble a swap plan with min_out.
#[tokio::test]
async fn router_builds_hops2_plan_with_min_out() {
    // Use dummy RPC URL to avoid any network access in tests
    let rpc = Arc::new(SolanaRpc::new("http://localhost:0"));

    // Two independent Orca connectors (both implement Dex)
    let orca0 = Arc::new(Orca::new(rpc.clone()));
    let orca1 = Arc::new(Orca::new(rpc.clone()));

    // Deterministic mints
    let a = Pubkey::new_from_array([10u8; 32]);
    let b = Pubkey::new_from_array([11u8; 32]);
    let c = Pubkey::new_from_array([12u8; 32]);

    // Insert mock pools
    // orca0: A <-> B with ample reserves
    orca0.insert_mock_pool(a, b, 1_000_000_000u128, 2_000_000_000u128, 30);
    // orca1: B <-> C with ample reserves
    orca1.insert_mock_pool(b, c, 2_000_000_000u128, 3_000_000_000u128, 30);

    // Prepare user authority and token accounts for both connectors (required by build_swap_ix)
    let auth = Pubkey::new_unique();
    for o in [&orca0, &orca1] {
        o.set_user_authority(auth);
        o.set_user_token_account(a, Pubkey::new_unique());
        o.set_user_token_account(b, Pubkey::new_unique());
        o.set_user_token_account(c, Pubkey::new_unique());
    }

    // Router with both Orca connectors
    let router = Router::new(vec![orca0.clone() as Arc<dyn Dex>, orca1.clone() as Arc<dyn Dex>]);

    let amount_in: u64 = 100_000; // input amount in A
    let slippage_bps: u32 = 100; // 1%

    // Build best 2-hop plan A -> B -> C
    let plan = router
        .build_best_hops2_plan_exact_in(&a.to_string(), &c.to_string(), amount_in, slippage_bps)
        .await
        .unwrap();
    assert!(plan.is_some(), "expected a multi-hop plan");
    let plan = plan.unwrap();

    // Expect 2 hops with non-zero expected_out and min_out (min_out applies slippage to final out)
    assert_eq!(plan.hops.len(), 2, "expected two-hop plan");
    assert!(plan.expected_out > 0, "expected_out must be positive");
    let expected_min = (plan.expected_out as u128 * (10_000u128 - slippage_bps as u128)
        / 10_000u128) as u64;
    assert_eq!(plan.min_out, expected_min, "min_out should apply 1% slippage to final out");

    // Plan should contain two swap instructions (one per hop) for Orca
    assert_eq!(plan.ixs.len(), 2, "should build two instructions (one per hop)");
}
