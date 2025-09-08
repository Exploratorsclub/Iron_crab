#![cfg(feature = "test_helpers")]

use ironcrab::solana::sniper::{test_compute_concentration, HolderClass};

#[test]
fn lp_concentration_excludes_burn_and_program_vaults() {
    // Synthetic: total supply 1000 tokens, of which 300 burned, 200 program-locked.
    // Regular holders: A=250, B=150, C=100.
    let total_supply = 1000.0;
    let holders = vec![
        (300.0, HolderClass::Burn),
        (200.0, HolderClass::ProgramVault),
        (250.0, HolderClass::Regular),
        (150.0, HolderClass::Regular),
        (100.0, HolderClass::Regular),
    ];
    let (top1, top3, top5, burned_pct, locked_pct) = test_compute_concentration(total_supply, &holders);
    assert!((top1 - 0.5).abs() < 1e-9); // 250 / (1000-300-200) = 0.5
    assert!((top3 - 1.0).abs() < 1e-9);
    assert!((top5 - 1.0).abs() < 1e-9);
    assert!((burned_pct - 0.3).abs() < 1e-9);
    assert!((locked_pct - 0.2).abs() < 1e-9);
}

#[test]
fn lp_concentration_thresholds_gate() {
    // Verify that a tight threshold fails the concentration_ok equivalent.
    let total_supply = 1000.0;
    let holders = vec![
        (300.0, HolderClass::Burn),
        (200.0, HolderClass::ProgramVault),
        (250.0, HolderClass::Regular),
        (150.0, HolderClass::Regular),
        (100.0, HolderClass::Regular),
    ];
    let (top1, top3, top5, _burned, _locked) = test_compute_concentration(total_supply, &holders);
    let ok = top1 <= 0.49 && top3 <= 0.99 && top5 <= 0.99;
    assert!(!ok);
}
