use ironcrab::solana::dex::pumpfun::PumpFunDex;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::{execution::tx_builder, ipc};
use solana_sdk::instruction::AccountMeta;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

#[tokio::test]
async fn test_pumpfun_build_buy_ix_pure_derivation() {
    // This test asserts we can build a Pump.fun BUY instruction list without any RPC calls.
    // It relies only on deterministic PDA + ATA derivations.

    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:8899"));
    let mut dex = PumpFunDex::new(rpc, None).expect("PumpFunDex::new");

    let wallet =
        Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM").expect("wallet pubkey");
    dex.set_user_authority(wallet);

    // Any syntactically valid pubkeys are fine for this pure-derivation test.
    let creator =
        Pubkey::from_str("2tFqgkJX6kqz8q6o9tFv3oJ9nQx7n1m3fHk2m8f3oKpZ").expect("creator pubkey");
    let token_mint = "9xQeWvG816bUx9EPfKJb9N9dKz5wW7Yy2hBzXv4mQ4kG"; // arbitrary valid pubkey string

    let ixs = dex
        .build_swap_ix_async_with_slippage(
            "So11111111111111111111111111111111111111112",
            token_mint,
            1_000_000, // 0.001 SOL
            123_456,   // min_out (raw)
            Some(creator),
            500,   // 5% slippage
            None,  // token_program_override - use default SPL Token
            false, // market_order: limit order
            false, // allow_rpc_fallback: Hot Path (pure-derivation test)
        )
        .await
        .expect("build_swap_ix_async_with_slippage");

    // Expect 2 instructions: ATA creation (idempotent) + pump.fun swap
    assert_eq!(ixs.len(), 2, "expected ATA creation + pump.fun instruction");

    // The second instruction is the pump.fun BUY
    let ix = &ixs[1];
    assert_eq!(
        ix.program_id,
        Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
            .expect("pumpfun program id"),
        "program_id must be pump.fun"
    );

    // The BUY ix uses AccountMeta::new(user, true) at index 6.
    let user_meta = ix.accounts.get(6).expect("user meta index 6");
    assert_eq!(user_meta.pubkey, wallet);
    assert!(user_meta.is_signer);
    assert!(user_meta.is_writable);

    // Sanity: at least one other account is writable (fee recipient).
    assert!(
        ix.accounts
            .iter()
            .any(|m| m.is_writable && m.pubkey != wallet),
        "expected at least one writable non-user account"
    );

    // Ensure we didn't accidentally construct empty data.
    assert!(!ix.data.is_empty(), "instruction data must not be empty");

    // IDL `buy`: discriminator + amount + max_sol_cost + track_volume (OptionBool = 1 byte)
    assert_eq!(
        ix.data.len(),
        25,
        "BUY instruction data must be 25 bytes (incl. track_volume OptionBool)"
    );
    assert_eq!(
        *ix.data.last().expect("data non-empty"),
        0u8,
        "track_volume OptionBool(false) must serialize as final byte 0"
    );
    let buy_disc: [u8; 8] = ix.data[0..8].try_into().expect("discriminator");
    assert_eq!(
        buy_disc,
        [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea],
        "limit BUY must use buy discriminator"
    );

    // Post-cashback-upgrade (Feb 2026): BUY requires 17 accounts (bonding_curve_v2 as last).
    assert_eq!(
        ix.accounts.len(),
        17,
        "BUY ix must have 17 accounts (bonding_curve_v2 required since Feb 2026)"
    );

    // Keep AccountMeta imported to avoid unused import warning changes across Solana versions.
    let _ = AccountMeta::new_readonly(Pubkey::default(), false);
}

#[tokio::test]
async fn test_pumpfun_market_order_buy() {
    // Market order uses buy_exact_sol_in discriminator [56, 252, 116, 8, 158, 223, 205, 95]
    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:8899"));
    let mut dex = PumpFunDex::new(rpc, None).expect("PumpFunDex::new");

    let wallet =
        Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM").expect("wallet pubkey");
    dex.set_user_authority(wallet);

    let creator =
        Pubkey::from_str("2tFqgkJX6kqz8q6o9tFv3oJ9nQx7n1m3fHk2m8f3oKpZ").expect("creator pubkey");
    let token_mint = "9xQeWvG816bUx9EPfKJb9N9dKz5wW7Yy2hBzXv4mQ4kG";

    let ixs = dex
        .build_swap_ix_async_with_slippage(
            "So11111111111111111111111111111111111111112",
            token_mint,
            1_000_000,
            1, // min_out ignored for market order
            Some(creator),
            500,
            None,
            true,  // market_order: exact SOL in, min tokens out = 1
            false, // allow_rpc_fallback: Hot Path
        )
        .await
        .expect("build_swap_ix_async_with_slippage");

    assert_eq!(ixs.len(), 2, "expected ATA creation + pump.fun instruction");
    let ix = &ixs[1];
    assert!(
        ix.data.len() >= 8,
        "instruction data must have at least 8-byte discriminator"
    );
    let discriminator: [u8; 8] = ix.data[0..8].try_into().expect("8 bytes");
    assert_eq!(
        discriminator,
        [56, 252, 116, 8, 158, 223, 205, 95],
        "market order must use buy_exact_sol_in discriminator"
    );
    assert_eq!(
        ix.data.len(),
        25,
        "buy_exact_sol_in data must be 25 bytes (incl. track_volume OptionBool)"
    );
    assert_eq!(
        *ix.data.last().expect("data non-empty"),
        0u8,
        "track_volume OptionBool(false) must serialize as final byte 0"
    );
}

#[tokio::test]
async fn test_tx_builder_supports_pumpfun_sell_pure_derivation() {
    // This test asserts the deterministic TxBuilder supports Pump.fun SELL intents.
    // It must be pure-derivation (no network required), relying only on PDA + ATA derivations.

    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:8899"));

    let wallet =
        Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM").expect("wallet pubkey");

    let creator = "2tFqgkJX6kqz8q6o9tFv3oJ9nQx7n1m3fHk2m8f3oKpZ";
    let token_mint = "9xQeWvG816bUx9EPfKJb9N9dKz5wW7Yy2hBzXv4mQ4kG";
    let sol_mint = "So11111111111111111111111111111111111111112";

    let mut intent = ipc::TradeIntent::new(
        "test",
        "test",
        "test",
        "intent-sell-derivation".to_string(),
        "test",
        ipc::IntentTier::Tier1,
        ipc::IntentOrigin::StrategyA,
        ipc::ExplicitAmount::new(1_000_000, 6), // 1.0 token (arbitrary decimals)
        ipc::TradeResources {
            input_mint: token_mint.to_string(),
            output_mint: sol_mint.to_string(),
            pools: vec!["pumpfun".to_string()],
            accounts: vec![],
            token_program: None,
        },
        0,
        500,
        ipc::TradeSide::Sell,
        ipc::TradingRegime::NotApplicable,
    );

    intent
        .metadata
        .insert("creator".to_string(), creator.to_string());
    intent
        .metadata
        .insert("min_out_raw".to_string(), "1".to_string()); // 1 lamport min SOL out

    let plan = match tx_builder::build_tx_plan(&intent, wallet, Arc::clone(&rpc), None, None, false)
        .await
    {
        tx_builder::TxPlanOutcome::Planned(p) => p,
        tx_builder::TxPlanOutcome::Unsupported(u) => {
            panic!(
                "unexpected unsupported plan: {:?} - {}",
                u.reason, u.details
            )
        }
    };

    // Expect 2 instructions: ATA creation (idempotent) + pump.fun SELL
    assert_eq!(
        plan.instructions.len(),
        2,
        "expected ATA creation + pump.fun instruction"
    );

    // The second instruction is the pump.fun SELL
    let ix = &plan.instructions[1];
    assert_eq!(
        ix.program_id,
        Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
            .expect("pumpfun program id"),
        "program_id must be pump.fun"
    );

    // The SELL ix uses AccountMeta::new(user, true) at index 6.
    let user_meta = ix.accounts.get(6).expect("user meta index 6");
    assert_eq!(user_meta.pubkey, wallet);
    assert!(user_meta.is_signer);
    assert!(user_meta.is_writable);

    assert!(!ix.data.is_empty(), "instruction data must not be empty");

    // Post-cashback-upgrade (Feb 2026): SELL has 15 (non-cashback) or 16 (cashback) accounts.
    assert!(
        ix.accounts.len() >= 15,
        "SELL ix must have at least 15 accounts (bonding_curve_v2 required since Feb 2026)"
    );
}

/// Regression test for Bug #25: Cold Path must verify cashback_enabled via RPC even on Cache-HIT.
/// When allow_rpc_fallback=true and RPC is unreachable, build must return Err (no silent fallback
/// to stale cache with cashback_enabled=false).
/// Ignored by default: uses unreachable RPC, can take 30s+ due to retries. Run with:
///   cargo test test_pumpfun_cold_path_rpc_verification_required -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn test_pumpfun_cold_path_rpc_verification_required() {
    // Use unreachable RPC — Cold Path requires verification, must fail clearly
    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:1"));
    let mut dex = PumpFunDex::new(rpc, None).expect("PumpFunDex::new");

    let wallet =
        Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM").expect("wallet pubkey");
    dex.set_user_authority(wallet);

    let creator =
        Pubkey::from_str("2tFqgkJX6kqz8q6o9tFv3oJ9nQx7n1m3fHk2m8f3oKpZ").expect("creator pubkey");
    let token_mint = "9xQeWvG816bUx9EPfKJb9N9dKz5wW7Yy2hBzXv4mQ4kG";

    let result = dex
        .build_swap_ix_async_with_slippage(
            "So11111111111111111111111111111111111111112",
            token_mint,
            1_000_000,
            123_456,
            Some(creator),
            500,
            None,
            false, // market_order
            true,  // allow_rpc_fallback: Cold Path — must verify via RPC
        )
        .await;

    // Cold Path with unreachable RPC must return Err, not silently use stale cache
    let err = result
        .expect_err("Cold Path with unreachable RPC must fail (Err), not succeed with stale cache");
    let err_str = err.to_string();
    assert!(
        err_str.contains("Cold Path") || err_str.contains("RPC verification"),
        "error should mention Cold Path / RPC verification: {}",
        err_str
    );
}
