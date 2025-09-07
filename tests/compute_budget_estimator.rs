use ironcrab::solana::compute_budget_estimator::{
    estimate_from_instructions, estimate_single_swap, EstimatorConfig,
};
use solana_sdk::instruction::Instruction;

#[test]
fn single_swap_estimate_in_range() {
    let est = estimate_single_swap(500_000_000);
    assert!(est.compute_unit_limit >= 80_000 && est.compute_unit_limit <= 400_000);
    assert_eq!(est.compute_unit_price_micro_lamports, 1);
}

#[test]
fn large_notional_increases_price() {
    let cfg = EstimatorConfig::default();
    let dummy_ix = Instruction {
        program_id: solana_sdk::pubkey::Pubkey::new_unique(),
        accounts: vec![],
        data: vec![],
    };
    let est = estimate_from_instructions(
        &[dummy_ix.clone(), dummy_ix],
        1,
        cfg.large_notional_threshold,
        cfg,
    );
    assert!(
        est.compute_unit_price_micro_lamports
            >= cfg.default_cu_price_micro_lamports * cfg.large_notional_multiplier
    );
}
