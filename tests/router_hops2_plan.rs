use ironcrab::execution::live_pool_cache::{
    CachedPoolState, LivePoolCache, OrcaWhirlpoolState, SharedLivePoolCache,
};
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

    // Deterministic mints
    let a = Pubkey::new_from_array([10u8; 32]);
    let b = Pubkey::new_from_array([11u8; 32]);
    let c = Pubkey::new_from_array([12u8; 32]);

    // LivePoolCache with both pools (GEYSER-FIRST, no RPC)
    let cache: SharedLivePoolCache = Arc::new(LivePoolCache::new());
    cache.upsert(
        a, // pool A-B keyed by a (insert_mock_pool convention)
        CachedPoolState::Orca(OrcaWhirlpoolState {
            token_mint_a: a,
            token_mint_b: b,
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            tick_current_index: 0,
            sqrt_price: 1,
            liquidity: 1,
            fee_rate: 300,
            protocol_fee_rate: 0,
            tick_spacing: 64,
            vault_a_balance: Some(1_000_000_000),
            vault_b_balance: Some(2_000_000_000),
            token_a_program: None,
            token_b_program: None,
        }),
        100,
    );
    cache.upsert(
        b, // pool B-C keyed by b
        CachedPoolState::Orca(OrcaWhirlpoolState {
            token_mint_a: b,
            token_mint_b: c,
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            tick_current_index: 0,
            sqrt_price: 1,
            liquidity: 1,
            fee_rate: 300,
            protocol_fee_rate: 0,
            tick_spacing: 64,
            vault_a_balance: Some(2_000_000_000),
            vault_b_balance: Some(3_000_000_000),
            token_a_program: None,
            token_b_program: None,
        }),
        100,
    );

    // Two independent Orca connectors with shared cache (Hot Path: no RPC on cache hit)
    let orca0 = Arc::new(Orca::new_with_cache(rpc.clone(), None, Some(cache.clone())));
    let orca1 = Arc::new(Orca::new_with_cache(rpc.clone(), None, Some(cache.clone())));

    // Inject pool state into both Orca instances
    let state_ab = OrcaWhirlpoolState {
        token_mint_a: a,
        token_mint_b: b,
        token_vault_a: Pubkey::new_unique(),
        token_vault_b: Pubkey::new_unique(),
        tick_current_index: 0,
        sqrt_price: 1,
        liquidity: 1,
        fee_rate: 300,
        protocol_fee_rate: 0,
        tick_spacing: 64,
        vault_a_balance: Some(1_000_000_000),
        vault_b_balance: Some(2_000_000_000),
        token_a_program: None,
        token_b_program: None,
    };
    let state_bc = OrcaWhirlpoolState {
        token_mint_a: b,
        token_mint_b: c,
        token_vault_a: Pubkey::new_unique(),
        token_vault_b: Pubkey::new_unique(),
        tick_current_index: 0,
        sqrt_price: 1,
        liquidity: 1,
        fee_rate: 300,
        protocol_fee_rate: 0,
        tick_spacing: 64,
        vault_a_balance: Some(2_000_000_000),
        vault_b_balance: Some(3_000_000_000),
        token_a_program: None,
        token_b_program: None,
    };
    orca0
        .inject_cached_orca_state(&a, &state_ab)
        .expect("inject ab");
    orca1
        .inject_cached_orca_state(&b, &state_bc)
        .expect("inject bc");

    // Prepare user authority and token accounts for both connectors (required by build_swap_ix)
    let auth = Pubkey::new_unique();
    for o in [&orca0, &orca1] {
        o.set_user_authority(auth);
        o.set_user_token_account(a, Pubkey::new_unique());
        o.set_user_token_account(b, Pubkey::new_unique());
        o.set_user_token_account(c, Pubkey::new_unique());
    }

    // Router with both Orca connectors
    let router = Router::new(vec![
        orca0.clone() as Arc<dyn Dex>,
        orca1.clone() as Arc<dyn Dex>,
    ]);

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
    let expected_min =
        (plan.expected_out as u128 * (10_000u128 - slippage_bps as u128) / 10_000u128) as u64;
    assert_eq!(
        plan.min_out, expected_min,
        "min_out should apply 1% slippage to final out"
    );

    // Plan should contain two swap instructions (one per hop) for Orca
    assert_eq!(
        plan.ixs.len(),
        2,
        "should build two instructions (one per hop)"
    );
}
