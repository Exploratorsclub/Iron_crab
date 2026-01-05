use ironcrab::solana::dex::orca::Orca;
use ironcrab::solana::dex::orca_whirlpool_layout::WhirlpoolParsed;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

#[tokio::test]
async fn test_orca_build_swap_ix_pure_derivation_with_inserted_whirlpool() {
    // This test asserts we can build an Orca swap instruction list without any RPC calls.
    // It relies on deterministic PDA derivations (tick arrays + oracle) and explicit user accounts.

    let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:8899"));
    let orca = Orca::new(rpc);

    let wallet =
        Pubkey::from_str("Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM").expect("wallet pubkey");
    orca.set_user_authority(wallet);

    let mint_a = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
    let mint_b = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap();

    // Any syntactically valid token accounts are fine for this pure-derivation test.
    let user_ata_a = Pubkey::new_unique();
    let user_ata_b = Pubkey::new_unique();
    orca.set_user_token_account(mint_a, user_ata_a);
    orca.set_user_token_account(mint_b, user_ata_b);

    let whirlpool_id = Pubkey::new_unique();
    let parsed = WhirlpoolParsed {
        token_mint_a: mint_a,
        token_mint_b: mint_b,
        token_vault_a: Pubkey::new_unique(),
        token_vault_b: Pubkey::new_unique(),
        fee_rate: 300,
        protocol_fee_rate: 0,
        tick_spacing: 64,
        tick_current_index: 0,
        liquidity: 1,
        sqrt_price: 1,
    };
    orca.insert_whirlpool_parsed(whirlpool_id, parsed);

    let ixs = orca
        .build_swap_ix(&mint_a.to_string(), &mint_b.to_string(), 1_000, 1)
        .expect("build_swap_ix");

    assert_eq!(ixs.len(), 1, "expected exactly one instruction");
    let ix = &ixs[0];

    // user authority is a signer in the account list
    assert!(ix
        .accounts
        .iter()
        .any(|m| m.pubkey == wallet && m.is_signer));
    // user token accounts appear as writable
    assert!(ix
        .accounts
        .iter()
        .any(|m| m.pubkey == user_ata_a && m.is_writable));
    assert!(ix
        .accounts
        .iter()
        .any(|m| m.pubkey == user_ata_b && m.is_writable));
    assert!(!ix.data.is_empty(), "instruction data must not be empty");
}
