// Run with: cargo test --test debug_burunduk_vault -- --nocapture

use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

#[test]
fn test_burunduk_creator_vault() {
    // Pump.fun program ID
    let program_id = Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap();

    println!("\n=== BURUNDUK TOKEN CREATOR VAULT ANALYSIS ===\n");

    // From the failed BUY transaction (2vXNqDLb...)
    // Error: ConstraintSeeds on creator_vault
    // Left (bot sends):  9HXMFwiLUpTRapAKsiiSPK3a3UqcqdWpEhrmvtsDugcC
    // Right (expected):  J7w9yXLLUKeVodeKkBJuCwt5DCubieBX69ebQntN8our

    let bot_sends_vault =
        Pubkey::from_str("9HXMFwiLUpTRapAKsiiSPK3a3UqcqdWpEhrmvtsDugcC").unwrap();
    let expected_vault = Pubkey::from_str("J7w9yXLLUKeVodeKkBJuCwt5DCubieBX69ebQntN8our").unwrap();

    println!("Bot sends creator_vault:  {}", bot_sends_vault);
    println!("Pump.fun expects:         {}", expected_vault);
    println!();

    // Creator extracted from Geyser CREATE tx (account index 7)
    let creator_from_geyser =
        Pubkey::from_str("6JaLPLDYpaZ2RPh1vYeStbWDZEEGdyZ62KhgYeiXZGos").unwrap();

    // Token mint
    let mint = Pubkey::from_str("EjBy3VxK7wh7idnCidDK1yxGndWdh4PE29RNPpGsR4Aa").unwrap();

    // Bonding curve
    let bonding_curve =
        Pubkey::from_str("3yH3TtmGECKM8xZYcqRLPnL59NUSrkFh7qSsDBEKWoam").unwrap();

    println!("Creator from Geyser:      {}", creator_from_geyser);
    println!("Token mint:               {}", mint);
    println!("Bonding curve:            {}", bonding_curve);
    println!();

    // Test 1: Derive from creator_from_geyser
    let (vault_from_creator, bump1) = Pubkey::find_program_address(
        &[b"creator-vault", creator_from_geyser.as_ref()],
        &program_id,
    );
    println!(
        "Vault from creator_from_geyser: {} (bump: {})",
        vault_from_creator, bump1
    );
    println!(
        "  Matches bot_sends?     {}",
        vault_from_creator == bot_sends_vault
    );
    println!(
        "  Matches expected?      {}",
        vault_from_creator == expected_vault
    );
    println!();

    // Test 2: Derive from mint
    let (vault_from_mint, bump2) =
        Pubkey::find_program_address(&[b"creator-vault", mint.as_ref()], &program_id);
    println!(
        "Vault from MINT:                {} (bump: {})",
        vault_from_mint, bump2
    );
    println!(
        "  Matches expected?      {}",
        vault_from_mint == expected_vault
    );
    println!();

    // Test 3: Derive from bonding curve
    let (vault_from_bc, bump3) =
        Pubkey::find_program_address(&[b"creator-vault", bonding_curve.as_ref()], &program_id);
    println!(
        "Vault from bonding_curve:       {} (bump: {})",
        vault_from_bc, bump3
    );
    println!(
        "  Matches expected?      {}",
        vault_from_bc == expected_vault
    );
    println!();

    // Now let's figure out what seed produces bot_sends_vault
    // and what seed produces expected_vault
    println!("=== REVERSE ENGINEERING ===\n");

    // The bot sends 9HXMFwiLUpTRapAKsiiSPK3a3UqcqdWpEhrmvtsDugcC
    // This is derived from SOME pubkey using ["creator-vault", pubkey]
    // Let's check all accounts from the CREATE tx

    let accounts = vec![
        ("index 0 (mint)", "EjBy3VxK7wh7idnCidDK1yxGndWdh4PE29RNPpGsR4Aa"),
        ("index 1", "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM"),
        (
            "index 2 (bonding_curve)",
            "3yH3TtmGECKM8xZYcqRLPnL59NUSrkFh7qSsDBEKWoam",
        ),
        ("index 3", "5aehzdAV6aPD5NBs7BSYEj5LJEXU1DkYVCvVX4VGYfV22"),
        (
            "index 4 (global)",
            "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf",
        ),
        (
            "index 5 (metaplex)",
            "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
        ),
        ("index 6", "Dt1Pb2x8daE4qnABcPFhrDvpnLGAQ6JRHBoKbktFzPe2"),
        (
            "index 7 (creator?)",
            "6JaLPLDYpaZ2RPh1vYeStbWDZEEGdyZ62KhgYeiXZGos",
        ),
    ];

    println!("Checking which account produces bot_sends_vault...\n");
    for (name, addr) in &accounts {
        if let Ok(pubkey) = Pubkey::from_str(addr) {
            let (derived, _) =
                Pubkey::find_program_address(&[b"creator-vault", pubkey.as_ref()], &program_id);
            if derived == bot_sends_vault {
                println!("*** FOUND: {} ({}) produces bot_sends_vault! ***", name, addr);
            }
            if derived == expected_vault {
                println!(
                    "*** FOUND: {} ({}) produces expected_vault! ***",
                    name, addr
                );
            }
        }
    }

    println!();
    println!("=== CONCLUSION ===\n");
    println!("If nothing matches, the seed is from a different source.");
    println!("The seed might be stored in the bonding curve account data itself.");
}
