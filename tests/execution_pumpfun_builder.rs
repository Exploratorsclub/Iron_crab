use ironcrab::solana::dex::pumpfun::PumpFunDex;
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

#[tokio::test]
async fn test_pumpfun_build_buy_ix_pure_derivation() {
    // This test asserts we can build a Pump.fun BUY instruction list without any RPC calls.
    // It relies only on deterministic PDA + ATA derivations.

    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:8899"));
    let mut dex = PumpFunDex::new(rpc).expect("PumpFunDex::new");

    let wallet = Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM")
        .expect("wallet pubkey");
    dex.set_user_authority(wallet);

    // Any syntactically valid pubkeys are fine for this pure-derivation test.
    let creator = Pubkey::from_str("2tFqgkJX6kqz8q6o9tFv3oJ9nQx7n1m3fHk2m8f3oKpZ")
        .expect("creator pubkey");
    let token_mint = "9xQeWvG816bUx9EPfKJb9N9dKz5wW7Yy2hBzXv4mQ4kG"; // arbitrary valid pubkey string

    let ixs = dex
        .build_swap_ix_async_with_slippage(
            "So11111111111111111111111111111111111111112",
            token_mint,
            1_000_000, // 0.001 SOL
            123_456,   // min_out (raw)
            Some(creator),
            500, // 5% slippage
        )
        .await
        .expect("build_swap_ix_async_with_slippage");

    assert_eq!(ixs.len(), 1, "expected exactly one instruction");

    let ix = &ixs[0];
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
        ix.accounts.iter().any(|m| m.is_writable && m.pubkey != wallet),
        "expected at least one writable non-user account"
    );

    // Ensure we didn't accidentally construct empty data.
    assert!(!ix.data.is_empty(), "instruction data must not be empty");

    // Keep AccountMeta imported to avoid unused import warning changes across Solana versions.
    let _ = AccountMeta::new_readonly(Pubkey::default(), false);
}
