use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

// Helper to fabricate a Raydium instance with an injected pool snapshot.
fn mk_raydium_with_pool(_base_res: u128, _quote_res: u128, _fee_bps: u32) -> Raydium {
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    // Direct insert via internal map not exposed; use snapshots() ingestion pattern mimic by calling refresh not feasible here.
    // Instead, we'll construct a PoolSnapshot and manually push through a minimal internal API if later exposed.
    // For now test only presence of builder returning None without pools.
    Raydium::new(rpc)
}

#[test]
fn swap_plan_without_pools_returns_none() {
    let r = mk_raydium_with_pool(0, 0, 25);
    let plan = r
        .build_swap_plan(
            &Pubkey::new_unique().to_string(),
            &Pubkey::new_unique().to_string(),
            1_000,
            50,
            None,
            None,
        )
        .unwrap();
    assert!(plan.is_none(), "expected no plan without pools");
}
