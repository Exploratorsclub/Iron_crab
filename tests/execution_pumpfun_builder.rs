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
            500,  // 5% slippage
            None, // token_program_override - use default SPL Token
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

    // Keep AccountMeta imported to avoid unused import warning changes across Solana versions.
    let _ = AccountMeta::new_readonly(Pubkey::default(), false);
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

    let plan = match tx_builder::build_tx_plan(&intent, wallet, Arc::clone(&rpc), None).await {
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
}
