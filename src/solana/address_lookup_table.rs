//! Address Lookup Table (ALT) support for transaction size reduction.
//!
//! Solana transactions are limited to 1232 bytes. Cross-DEX arbitrage transactions
//! easily exceed this limit due to the large number of accounts involved.
//!
//! Solution: Use Address Lookup Tables (ALT) to reference accounts by index (1 byte)
//! instead of full pubkey (32 bytes). This reduces TX size by ~60-70%.
//!
//! Architecture compliance:
//! - ALT is created once via setup tool (not hot path)
//! - execution-engine loads ALT at startup
//! - Only execution-engine signs/sends (Single-Signer)

use anyhow::{anyhow, Context, Result};
use solana_address_lookup_table_interface::{
    instruction as alt_instruction,
    state::AddressLookupTable,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_message::{v0, AddressLookupTableAccount, VersionedMessage};
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Signer,
    transaction::VersionedTransaction,
};
use std::str::FromStr;
use tracing::info;

/// Well-known program IDs and accounts that should be in every ALT.
/// These are used in almost every transaction.
pub const COMMON_ACCOUNTS: &[&str] = &[
    // System programs
    "11111111111111111111111111111111",                      // System Program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",          // Token Program
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",         // Associated Token Program
    "ComputeBudget111111111111111111111111111111",          // Compute Budget Program
    "SysvarRent111111111111111111111111111111111",          // Rent Sysvar
    "SysvarC1ock11111111111111111111111111111111",          // Clock Sysvar
    // WSOL mint
    "So11111111111111111111111111111111111111112",          // Wrapped SOL
    // DEX Programs
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",         // Pump.fun Bonding Curve
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",         // PumpSwap AMM
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",         // Orca Whirlpool
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",         // Meteora DLMM
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",        // Raydium AMM V4
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",        // Raydium CPMM
    // Token-2022 (some tokens use this)
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",         // Token-2022 Program
];

/// Loaded ALT data for use in transaction building.
#[derive(Debug, Clone)]
pub struct LoadedAlt {
    pub address: Pubkey,
    pub accounts: Vec<Pubkey>,
}

impl LoadedAlt {
    /// Check if an account is in this ALT.
    pub fn contains(&self, pubkey: &Pubkey) -> bool {
        self.accounts.contains(pubkey)
    }

    /// Get the index of an account in this ALT (for debugging).
    pub fn index_of(&self, pubkey: &Pubkey) -> Option<usize> {
        self.accounts.iter().position(|p| p == pubkey)
    }
}

/// Load an existing ALT from chain.
pub async fn load_alt(rpc: &RpcClient, alt_address: &Pubkey) -> Result<LoadedAlt> {
    let account = rpc
        .get_account_with_commitment(alt_address, CommitmentConfig::confirmed())
        .await
        .context("failed to fetch ALT account")?
        .value
        .ok_or_else(|| anyhow!("ALT account not found: {}", alt_address))?;

    let lookup_table = AddressLookupTable::deserialize(&account.data)
        .map_err(|e| anyhow!("failed to deserialize ALT: {:?}", e))?;

    let accounts: Vec<Pubkey> = lookup_table.addresses.iter().copied().collect();

    info!(
        alt = %alt_address,
        accounts_count = accounts.len(),
        "Loaded Address Lookup Table"
    );

    Ok(LoadedAlt {
        address: *alt_address,
        accounts,
    })
}

/// Create instructions to initialize a new ALT.
/// Returns (create_ix, alt_address).
pub fn create_alt_instructions(
    authority: &Pubkey,
    recent_slot: u64,
) -> Result<(Instruction, Pubkey)> {
    let (ix, alt_address) = alt_instruction::create_lookup_table(*authority, *authority, recent_slot);
    Ok((ix, alt_address))
}

/// Create instruction to extend an ALT with new addresses.
pub fn extend_alt_instruction(
    alt_address: &Pubkey,
    authority: &Pubkey,
    payer: &Pubkey,
    new_addresses: Vec<Pubkey>,
) -> Instruction {
    alt_instruction::extend_lookup_table(*alt_address, *authority, Some(*payer), new_addresses)
}

/// Build a versioned transaction using an Address Lookup Table.
///
/// This converts a legacy message to a v0 message that references the ALT,
/// reducing transaction size significantly.
pub fn build_versioned_transaction(
    instructions: &[Instruction],
    payer: &Pubkey,
    signers: &[&dyn Signer],
    recent_blockhash: solana_sdk::hash::Hash,
    alt: Option<&LoadedAlt>,
) -> Result<VersionedTransaction> {
    // Build the v0 message
    let message = if let Some(loaded_alt) = alt {
        // Create address lookup table account for the message
        let alt_account = AddressLookupTableAccount {
            key: loaded_alt.address,
            addresses: loaded_alt.accounts.clone(),
        };

        v0::Message::try_compile(payer, instructions, &[alt_account], recent_blockhash)
            .map_err(|e| anyhow!("failed to compile v0 message: {}", e))?
    } else {
        // No ALT - build without lookup tables (will be larger)
        v0::Message::try_compile(payer, instructions, &[], recent_blockhash)
            .map_err(|e| anyhow!("failed to compile v0 message without ALT: {}", e))?
    };

    let versioned_message = VersionedMessage::V0(message);

    // Sign the transaction
    let tx = VersionedTransaction::try_new(versioned_message, signers)
        .map_err(|e| anyhow!("failed to create versioned transaction: {}", e))?;

    Ok(tx)
}

/// Estimate the size reduction from using an ALT.
pub fn estimate_size_reduction(
    instructions: &[Instruction],
    alt: &LoadedAlt,
) -> (usize, usize) {
    let mut accounts_in_alt = 0;
    let mut total_unique_accounts = std::collections::HashSet::new();

    for ix in instructions {
        for meta in &ix.accounts {
            total_unique_accounts.insert(meta.pubkey);
            if alt.contains(&meta.pubkey) {
                accounts_in_alt += 1;
            }
        }
    }

    // Each account in ALT saves ~31 bytes (32 byte pubkey -> 1 byte index)
    let bytes_saved = accounts_in_alt * 31;

    (accounts_in_alt, bytes_saved)
}

/// Parse common accounts from the constant list.
pub fn get_common_accounts() -> Vec<Pubkey> {
    COMMON_ACCOUNTS
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_accounts_parse() {
        let accounts = get_common_accounts();
        assert!(!accounts.is_empty());
        assert_eq!(accounts.len(), COMMON_ACCOUNTS.len());
    }

    #[test]
    fn test_loaded_alt_contains() {
        let accounts = get_common_accounts();
        let alt = LoadedAlt {
            address: Pubkey::new_unique(),
            accounts: accounts.clone(),
        };

        // System program should be in ALT
        let system_program = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        assert!(alt.contains(&system_program));

        // Random pubkey should not be in ALT
        assert!(!alt.contains(&Pubkey::new_unique()));
    }
}
