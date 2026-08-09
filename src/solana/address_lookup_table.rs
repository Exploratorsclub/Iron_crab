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
    instruction as alt_instruction, state::AddressLookupTable,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_message::{v0, AddressLookupTableAccount, VersionedMessage};
use solana_sdk::{
    instruction::Instruction, pubkey::Pubkey, signature::Signer, transaction::VersionedTransaction,
};
use std::str::FromStr;
use tracing::info;

/// Well-known PumpSwap AMM global accounts (same for all pools).
pub const PUMPSWAP_AMM_GLOBAL_CONFIG: &str = "ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw";
/// PumpSwap `__event_authority` PDA under `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`.
pub const PUMPSWAP_AMM_EVENT_AUTHORITY: &str = "GS4CU59F31iL7aR2Q8zVS8DRrcRnXX1yjQ66TqNVQnaR";
/// Pump.fun bonding-curve event authority (not the PumpSwap AMM PDA).
pub const PUMPFUN_BONDING_EVENT_AUTHORITY: &str = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1";
/// Former COMMON_ACCOUNTS entry — not PumpSwap global_config; kept for audit diffs only.
pub const REMOVED_WRONG_PUMPSWAP_GLOBAL_CONFIG: &str =
    "39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg";

/// Well-known program IDs and accounts that should be in every ALT.
/// These are used in almost every transaction.
pub const COMMON_ACCOUNTS: &[&str] = &[
    // ===== System Programs =====
    "11111111111111111111111111111111", // System Program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // Token Program
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", // Associated Token Program
    "ComputeBudget111111111111111111111111111111", // Compute Budget Program
    "SysvarRent111111111111111111111111111111111", // Rent Sysvar
    "SysvarC1ock11111111111111111111111111111111", // Clock Sysvar
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", // Token-2022 Program
    // ===== Mints =====
    "So11111111111111111111111111111111111111112", // Wrapped SOL
    // ===== Meteora DLMM =====
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", // Meteora DLMM Program
    "D1ZN9Wj1fRSUQfCjhvnu1hqDMT7hzjzBBpi12nVniYD6", // Meteora Event Authority PDA
    // ===== PumpSwap AMM =====
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", // PumpSwap AMM Program
    "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ", // PumpSwap Fee Program
    PUMPSWAP_AMM_GLOBAL_CONFIG,                    // PumpSwap global_config (swap ix account #2)
    "5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx", // PumpSwap Fee Config (global constant)
    PUMPSWAP_AMM_EVENT_AUTHORITY,                  // PumpSwap __event_authority PDA (swap ix #15)
    "C2aFPdENg4A2HQsmrd5rTw5TaYBX5Ku887cWjbFKtZpw", // PumpSwap global_volume_accumulator PDA
    // ===== Pump.fun Bonding Curve =====
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", // Pump.fun Bonding Program
    "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf", // Pump.fun Global Account
    "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM", // Pump.fun Fee Account
    PUMPFUN_BONDING_EVENT_AUTHORITY,               // Pump.fun bonding-curve event_authority
    // ===== Orca Whirlpool =====
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", // Orca Whirlpool Program
    // ===== Raydium =====
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", // Raydium AMM V4
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", // Raydium CPMM
    "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h", // Raydium AMM Authority
    // ===== Jito Tip Accounts (for bundle tips) =====
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5", // Jito Tip Account 1
    "HFqU5x63VTqvQss8hp11i4bVmyBkEr7SrFKerWRx5Qdr", // Jito Tip Account 2
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY", // Jito Tip Account 3
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49", // Jito Tip Account 4
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh", // Jito Tip Account 5
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt", // Jito Tip Account 6
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL", // Jito Tip Account 7
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT", // Jito Tip Account 8
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

/// Build an `AddressLookupTableAccount`, optionally excluding `tip_account`.
///
/// Jito bundle tips must be static writable keys in v0 messages. If the tip account
/// is referenced via ALT lookup, the writable flag cannot be set and Jito rejects the
/// bundle. Simulation and send must both use the same filtered ALT.
pub fn address_lookup_table_account(
    alt: &LoadedAlt,
    exclude_tip: Option<&Pubkey>,
) -> AddressLookupTableAccount {
    let addresses = match exclude_tip {
        Some(tip) => alt
            .accounts
            .iter()
            .filter(|addr| *addr != tip)
            .copied()
            .collect(),
        None => alt.accounts.clone(),
    };
    AddressLookupTableAccount {
        key: alt.address,
        addresses,
    }
}

/// Compile a v0 `VersionedMessage` with optional ALT and tip filtering.
pub fn compile_v0_versioned_message(
    payer: &Pubkey,
    instructions: &[Instruction],
    alt: Option<&LoadedAlt>,
    exclude_tip_from_alt: Option<&Pubkey>,
    recent_blockhash: solana_sdk::hash::Hash,
) -> Result<VersionedMessage, String> {
    let message = if let Some(alt) = alt {
        let alt_account = address_lookup_table_account(alt, exclude_tip_from_alt);
        v0::Message::try_compile(payer, instructions, &[alt_account], recent_blockhash)
            .map_err(|e| format!("v0_compile_error:{e}"))?
    } else {
        v0::Message::try_compile(payer, instructions, &[], recent_blockhash)
            .map_err(|e| format!("v0_compile_error:{e}"))?
    };
    Ok(VersionedMessage::V0(message))
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
    let (ix, alt_address) =
        alt_instruction::create_lookup_table(*authority, *authority, recent_slot);
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
pub fn estimate_size_reduction(instructions: &[Instruction], alt: &LoadedAlt) -> (usize, usize) {
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

/// Analysis of how a compiled v0 message uses (or misses) the loaded ALT.
#[derive(Debug, Clone)]
pub struct AltCompileAnalysis {
    /// Static account keys in the v0 message header.
    pub static_key_count: usize,
    /// Instruction account indices that resolve via address lookup tables.
    pub alt_hit_count: usize,
    /// Static keys not present in the loaded ALT (top-N for ops `setup_alt --extra-addresses`).
    pub static_not_in_alt: Vec<Pubkey>,
    /// Static keys that are present in the loaded ALT but were not compiled as lookups.
    ///
    /// Expected cases: fee payer (signer), Jito tip when `exclude_tip_from_alt` is set, and any
    /// account the v0 compiler must keep static (signers). A high count with low `alt_hit_count`
    /// usually means the on-chain ALT is missing pool-specific keys, not a compile bug.
    pub alt_in_table_but_static: Vec<Pubkey>,
    /// Total static keys that exist in the loaded ALT table.
    pub alt_in_table_but_static_count: usize,
}

/// Analyze ALT usage for a compiled v0 message.
pub fn analyze_v0_alt_usage(message: &v0::Message, alt: Option<&LoadedAlt>) -> AltCompileAnalysis {
    let static_key_count = message.account_keys.len();
    let mut alt_hit_count = 0usize;
    for ix in &message.instructions {
        for account_index in &ix.accounts {
            if *account_index as usize >= static_key_count {
                alt_hit_count += 1;
            }
        }
    }

    let static_not_in_alt: Vec<Pubkey> = match alt {
        Some(loaded) => message
            .account_keys
            .iter()
            .filter(|pk| !loaded.contains(pk))
            .take(10)
            .copied()
            .collect(),
        None => message.account_keys.clone(),
    };

    let alt_in_table_but_static_all: Vec<Pubkey> = match alt {
        Some(loaded) => message
            .account_keys
            .iter()
            .filter(|pk| loaded.contains(pk))
            .copied()
            .collect(),
        None => Vec::new(),
    };
    let alt_in_table_but_static_count = alt_in_table_but_static_all.len();
    let alt_in_table_but_static = alt_in_table_but_static_all.into_iter().take(10).collect();

    AltCompileAnalysis {
        static_key_count,
        alt_hit_count,
        static_not_in_alt,
        alt_in_table_but_static,
        alt_in_table_but_static_count,
    }
}

/// Analyze ALT usage from a versioned message (legacy messages have no lookups).
pub fn analyze_versioned_message_alt_usage(
    message: &VersionedMessage,
    alt: Option<&LoadedAlt>,
) -> AltCompileAnalysis {
    match message {
        VersionedMessage::V0(v0_msg) => analyze_v0_alt_usage(v0_msg, alt),
        VersionedMessage::Legacy(legacy) => AltCompileAnalysis {
            static_key_count: legacy.account_keys.len(),
            alt_hit_count: 0,
            static_not_in_alt: legacy.account_keys.clone(),
            alt_in_table_but_static: Vec::new(),
            alt_in_table_but_static_count: 0,
        },
    }
}

/// Returns duplicate pubkeys in a v0/legacy message static account key list (`AccountLoadedTwice` risk).
pub fn versioned_message_duplicate_static_keys(message: &VersionedMessage) -> Vec<Pubkey> {
    let keys = match message {
        VersionedMessage::V0(v0) => &v0.account_keys,
        VersionedMessage::Legacy(legacy) => &legacy.account_keys,
    };
    let mut seen = std::collections::HashSet::new();
    let mut dupes = Vec::new();
    for pk in keys {
        if !seen.insert(*pk) {
            dupes.push(*pk);
        }
    }
    dupes.sort();
    dupes.dedup();
    dupes
}

/// Parse common accounts from the constant list.
pub fn get_common_accounts() -> Vec<Pubkey> {
    COMMON_ACCOUNTS
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect()
}

/// Pubkeys in [`COMMON_ACCOUNTS`] that are missing from a loaded on-chain ALT.
pub fn common_accounts_missing_from_alt(alt: &LoadedAlt) -> Vec<Pubkey> {
    get_common_accounts()
        .into_iter()
        .filter(|pk| !alt.contains(pk))
        .collect()
}

/// Collect unique pubkeys referenced by instructions (program IDs + account metas).
pub fn collect_instruction_pubkeys(instructions: &[Instruction]) -> Vec<Pubkey> {
    let mut keys = Vec::new();
    for ix in instructions {
        keys.push(ix.program_id);
        for meta in &ix.accounts {
            keys.push(meta.pubkey);
        }
    }
    keys.sort();
    keys.dedup();
    keys
}

/// Global instruction pubkeys (from `COMMON_ACCOUNTS`) vs per-pool keys for bundle audits.
pub fn partition_globals_vs_pool_specific(
    instruction_keys: &[Pubkey],
    alt: &LoadedAlt,
) -> (Vec<Pubkey>, Vec<Pubkey>) {
    let common = get_common_accounts();
    let mut globals_missing = Vec::new();
    let mut pool_specific = Vec::new();
    for pk in instruction_keys {
        if common.contains(pk) {
            if !alt.contains(pk) {
                globals_missing.push(*pk);
            }
        } else {
            pool_specific.push(*pk);
        }
    }
    globals_missing.sort();
    globals_missing.dedup();
    pool_specific.sort();
    pool_specific.dedup();
    (globals_missing, pool_specific)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pumpswap_global_constants_match_pdas() {
        let program = Pubkey::from_str("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA").unwrap();
        let (event_authority, _) = Pubkey::find_program_address(&[b"__event_authority"], &program);
        let (gva, _) = Pubkey::find_program_address(&[b"global_volume_accumulator"], &program);
        assert_eq!(
            event_authority,
            Pubkey::from_str(PUMPSWAP_AMM_EVENT_AUTHORITY).unwrap()
        );
        assert_eq!(
            gva,
            Pubkey::from_str("C2aFPdENg4A2HQsmrd5rTw5TaYBX5Ku887cWjbFKtZpw").unwrap()
        );
        assert!(get_common_accounts().contains(&event_authority));
        assert!(
            get_common_accounts().contains(&Pubkey::from_str(PUMPSWAP_AMM_GLOBAL_CONFIG).unwrap())
        );
        assert!(!get_common_accounts()
            .contains(&Pubkey::from_str(REMOVED_WRONG_PUMPSWAP_GLOBAL_CONFIG).unwrap()));
    }

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

    #[test]
    fn address_lookup_table_account_filters_tip_from_alt() {
        let tip = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let alt = LoadedAlt {
            address: Pubkey::new_unique(),
            accounts: vec![tip, other],
        };

        let filtered = address_lookup_table_account(&alt, Some(&tip));
        assert_eq!(filtered.addresses.len(), 1);
        assert_eq!(filtered.addresses[0], other);

        let unfiltered = address_lookup_table_account(&alt, None);
        assert_eq!(unfiltered.addresses.len(), 2);
    }

    #[test]
    fn compile_v0_versioned_message_tip_filter_matches_send_form() {
        use solana_sdk::instruction::{AccountMeta, Instruction};
        use solana_system_program;

        let payer = Pubkey::new_unique();
        let tip = Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5").unwrap();
        let recipient = Pubkey::new_unique();
        let blockhash = solana_sdk::hash::Hash::new_unique();
        let alt = LoadedAlt {
            address: Pubkey::new_unique(),
            accounts: vec![tip, recipient],
        };
        let mut data = vec![2, 0, 0, 0];
        data.extend_from_slice(&1u64.to_le_bytes());
        let ix = Instruction {
            program_id: solana_system_program::id(),
            accounts: vec![AccountMeta::new(payer, true), AccountMeta::new(tip, false)],
            data,
        };

        let with_tip_in_alt = compile_v0_versioned_message(
            &payer,
            std::slice::from_ref(&ix),
            Some(&alt),
            None,
            blockhash,
        )
        .expect("compile with tip in ALT");
        let tip_filtered = compile_v0_versioned_message(
            &payer,
            std::slice::from_ref(&ix),
            Some(&alt),
            Some(&tip),
            blockhash,
        )
        .expect("compile with tip filtered from ALT");

        assert_ne!(with_tip_in_alt, tip_filtered);
    }

    #[test]
    fn analyze_v0_alt_usage_counts_static_and_lookup_refs() {
        use solana_sdk::instruction::{AccountMeta, Instruction};
        use solana_system_program;

        let payer = Pubkey::new_unique();
        let in_alt = Pubkey::new_unique();
        let static_only = Pubkey::new_unique();
        let blockhash = solana_sdk::hash::Hash::new_unique();
        let loaded = LoadedAlt {
            address: Pubkey::new_unique(),
            accounts: vec![in_alt],
        };
        let mut data = vec![2, 0, 0, 0];
        data.extend_from_slice(&1u64.to_le_bytes());
        let ix = Instruction {
            program_id: solana_system_program::id(),
            accounts: vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(in_alt, false),
                AccountMeta::new(static_only, false),
            ],
            data,
        };
        let message = compile_v0_versioned_message(
            &payer,
            std::slice::from_ref(&ix),
            Some(&loaded),
            None,
            blockhash,
        )
        .expect("compile");
        let v0_msg = match message {
            VersionedMessage::V0(m) => m,
            _ => panic!("expected v0"),
        };
        let analysis = analyze_v0_alt_usage(&v0_msg, Some(&loaded));
        assert!(analysis.static_key_count >= 2);
        assert!(analysis.alt_hit_count >= 1);
        assert!(analysis.static_not_in_alt.contains(&static_only));
    }

    #[test]
    fn analyze_v0_alt_usage_tracks_in_table_but_static() {
        use solana_sdk::instruction::{AccountMeta, Instruction};
        use solana_system_program;

        let payer = Pubkey::new_unique();
        let in_alt = Pubkey::new_unique();
        let blockhash = solana_sdk::hash::Hash::new_unique();
        let loaded = LoadedAlt {
            address: Pubkey::new_unique(),
            accounts: vec![payer, in_alt],
        };
        let mut data = vec![2, 0, 0, 0];
        data.extend_from_slice(&1u64.to_le_bytes());
        let ix = Instruction {
            program_id: solana_system_program::id(),
            accounts: vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(in_alt, false),
            ],
            data,
        };
        let message = compile_v0_versioned_message(
            &payer,
            std::slice::from_ref(&ix),
            Some(&loaded),
            None,
            blockhash,
        )
        .expect("compile");
        let v0_msg = match message {
            VersionedMessage::V0(m) => m,
            _ => panic!("expected v0"),
        };
        let analysis = analyze_v0_alt_usage(&v0_msg, Some(&loaded));
        assert!(
            analysis.alt_in_table_but_static_count >= 1,
            "signer payer stays static even when present in ALT"
        );
        assert!(analysis.alt_in_table_but_static.contains(&payer));
    }
}
