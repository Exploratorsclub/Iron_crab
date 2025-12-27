// Run with: cargo test --test verify_creator_vault -- --nocapture
// Or add as a bin target

use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

fn main() {
    // Pump.fun program ID
    let program_id = Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap();

    println!("=== PUMP.FUN CREATOR VAULT ANALYSIS ===\n");

    // From the failed transaction logs:
    // Left (passed):   2dLnUx769DxAXc7oTbbQkgHJTZ4W7yebCvVX4VGYfV22
    // Right (expected): J7w9yXLLUKeVodeKkBJuCwt5DCubieBX69ebQntN8our

    let passed_vault = Pubkey::from_str("2dLnUx769DxAXc7oTbbQkgHJTZ4W7yebCvVX4VGYfV22").unwrap();
    let expected_vault = Pubkey::from_str("J7w9yXLLUKeVodeKkBJuCwt5DCubieBX69ebQntN8our").unwrap();

    println!("Passed creator_vault:   {}", passed_vault);
    println!("Expected creator_vault: {}", expected_vault);
    println!();

    // Creator we read from bytes 49-81 of bonding curve
    let creator_from_bytes_49_81 =
        Pubkey::from_str("CMwrm79Pj6iYTv6EQYu3ibs3RcNgzibGVmB4bALhCfg6").unwrap();

    // Derive vault using this creator
    let (derived_vault, bump) = Pubkey::find_program_address(
        &[b"creator-vault", creator_from_bytes_49_81.as_ref()],
        &program_id,
    );

    println!("Creator from bytes 49-81: {}", creator_from_bytes_49_81);
    println!("Vault derived from it:    {}", derived_vault);
    println!("Bump: {}", bump);
    println!();

    if derived_vault == passed_vault {
        println!("✓ MATCH: This is the vault we passed in the TX");
        println!("  But it's WRONG - the program expected a different vault!");
    } else {
        println!("✗ MISMATCH: Something else is wrong");
    }
    println!();

    // Now let's figure out what creator would produce the EXPECTED vault
    // We need to reverse-engineer this...

    // Known values from TX:
    let mint = Pubkey::from_str("38BVpvWavTinkEgBjkx9cgMroibvXn5s2yXrCA742TSR").unwrap();
    let bonding_curve = Pubkey::from_str("2w9iyRiMhfNybwESBcTmU1XYeipJVCkxGiebx8TD5rZj").unwrap();

    println!("=== TESTING ALTERNATIVE DERIVATIONS ===\n");

    // Test: What if creator_vault is derived from MINT?
    let (vault_from_mint, _) =
        Pubkey::find_program_address(&[b"creator-vault", mint.as_ref()], &program_id);
    println!("Vault from MINT seed:     {}", vault_from_mint);
    if vault_from_mint == expected_vault {
        println!("*** MATCH! creator_vault is now derived from MINT, not creator! ***");
    }
    println!();

    // Test: What if creator_vault is derived from bonding_curve?
    let (vault_from_bc, _) =
        Pubkey::find_program_address(&[b"creator-vault", bonding_curve.as_ref()], &program_id);
    println!("Vault from BC seed:       {}", vault_from_bc);
    if vault_from_bc == expected_vault {
        println!("*** MATCH! creator_vault is derived from bonding curve! ***");
    }
    println!();

    // The fact that the expected vault is different means we need to find
    // what pubkey produces J7w9yXLLUKeVodeKkBJuCwt5DCubieBX69ebQntN8our
    // when used as [b"creator-vault", pubkey] seed

    println!("=== CONCLUSION ===\n");
    println!("The bytes at offset 49-81 in the bonding curve account");
    println!("NO LONGER contain the 'creator' pubkey.");
    println!();
    println!("The new 154-byte layout has different field ordering.");
    println!("Need to find the actual creator offset or alternative derivation.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vault_derivation() {
        main();
    }
}
