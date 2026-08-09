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
use solana_message::{
    v0, v0::LoadedAddresses, AccountKeys, AddressLookupTableAccount, VersionedMessage,
};
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
/// BPF-loader program account observed as stale `pool_accounts[1]` in prod (not PumpSwap global_config).
pub const PUMP_AMM_WRONG_LEGACY_GLOBAL_CONFIG: &str = "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt";
/// Prod on-chain ALT address (`setup-alt` / EE config).
pub const PROD_ON_CHAIN_ALT_ADDRESS: &str = "2J74oVKaviWzVr9gaLDvmBf3VAVVpzy88i3YCnyDwvuX";

/// Pubkeys that must not appear as static keys when PumpSwap ix meta #2 is canonical `global_config`.
pub const PUMP_AMM_KNOWN_BAD_GLOBAL_CONFIG_PUBKEYS: &[&str] = &[
    PUMP_AMM_WRONG_LEGACY_GLOBAL_CONFIG,
    REMOVED_WRONG_PUMPSWAP_GLOBAL_CONFIG,
];

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

/// Map filtered-ALT lookup indices (tip excluded from compile LUT) to on-chain ALT indices.
///
/// `try_compile` indexes into the `AddressLookupTableAccount::addresses` slice passed at compile
/// time. When the tip is removed so it stays a static writable key (#373), those indices drift
/// vs the on-chain LUT that RPC `simulateTransaction` resolves — remap after compile.
fn filtered_alt_index_to_on_chain_map(alt: &LoadedAlt, exclude_tip: &Pubkey) -> Option<Vec<u8>> {
    if !alt.contains(exclude_tip) {
        return None;
    }
    let map: Vec<u8> = alt
        .accounts
        .iter()
        .filter(|pk| *pk != exclude_tip)
        .filter_map(|pk| {
            let idx = alt.index_of(pk)?;
            u8::try_from(idx).ok()
        })
        .collect();
    Some(map)
}

/// Rewrite v0 lookup indices from filtered-ALT positions to on-chain ALT positions.
pub fn remap_v0_alt_lookup_indices_to_on_chain(
    message: &mut v0::Message,
    alt: &LoadedAlt,
    exclude_tip: &Pubkey,
) -> Result<(), String> {
    let Some(index_map) = filtered_alt_index_to_on_chain_map(alt, exclude_tip) else {
        return Ok(());
    };
    for lookup in &mut message.address_table_lookups {
        if lookup.account_key != alt.address {
            return Err(format!(
                "alt_lookup_remap_unexpected_table:{}",
                lookup.account_key
            ));
        }
        for idx in lookup
            .writable_indexes
            .iter_mut()
            .chain(lookup.readonly_indexes.iter_mut())
        {
            let filtered = *idx as usize;
            *idx = *index_map
                .get(filtered)
                .ok_or_else(|| format!("alt_lookup_remap_oob:filtered_index_{filtered}"))?;
        }
    }
    Ok(())
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
        let mut message =
            v0::Message::try_compile(payer, instructions, &[alt_account], recent_blockhash)
                .map_err(|e| format!("v0_compile_error:{e}"))?;
        if let Some(tip) = exclude_tip_from_alt {
            remap_v0_alt_lookup_indices_to_on_chain(&mut message, alt, tip)?;
        }
        message
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

/// Reconstruct loaded ALT addresses from a compiled v0 message and a known on-chain ALT snapshot.
///
/// Used for tests / diagnostics when verifying resolved instruction account pubkeys match ix metas.
pub fn loaded_addresses_from_alt_lookups(
    alt: &LoadedAlt,
    message: &VersionedMessage,
) -> Option<LoadedAddresses> {
    let VersionedMessage::V0(v0) = message else {
        return None;
    };
    if v0.address_table_lookups.is_empty() {
        return Some(LoadedAddresses::default());
    }
    let mut writable = Vec::new();
    let mut readonly = Vec::new();
    for lookup in &v0.address_table_lookups {
        if lookup.account_key != alt.address {
            continue;
        }
        for &idx in &lookup.writable_indexes {
            if let Some(pk) = alt.accounts.get(idx as usize) {
                writable.push(*pk);
            }
        }
        for &idx in &lookup.readonly_indexes {
            if let Some(pk) = alt.accounts.get(idx as usize) {
                readonly.push(*pk);
            }
        }
    }
    Some(LoadedAddresses { writable, readonly })
}

/// Resolve the pubkey at `meta_index` within compiled instruction `ix_index` of a versioned message.
pub fn resolved_instruction_account_pubkey(
    message: &VersionedMessage,
    loaded_addresses: Option<&LoadedAddresses>,
    ix_index: usize,
    meta_index: usize,
) -> Option<Pubkey> {
    match message {
        VersionedMessage::V0(v0) => {
            let ix = v0.instructions.get(ix_index)?;
            let key_index = *ix.accounts.get(meta_index)? as usize;
            let keys = AccountKeys::new(&v0.account_keys, loaded_addresses);
            keys.get(key_index).copied()
        }
        VersionedMessage::Legacy(legacy) => {
            let ix = legacy.instructions.get(ix_index)?;
            let key_index = *ix.accounts.get(meta_index)? as usize;
            legacy.account_keys.get(key_index).copied()
        }
    }
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

/// Structured failure from pre-sim PumpSwap `global_config` compile audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PumpAmmGlobalConfigAuditError {
    pub ix_index: usize,
    pub meta_pubkey: Pubkey,
    pub resolved_pubkey: Pubkey,
    pub static_contains_bad: Vec<Pubkey>,
}

impl PumpAmmGlobalConfigAuditError {
    pub fn reason_code(&self) -> &'static str {
        "pump_amm_compile_global_config_resolve_mismatch"
    }

    pub fn detail(&self) -> String {
        format!(
            "pump_amm_compile_global_config_resolve_mismatch:ix={}:meta={}:resolved={}:static_bad={:?}",
            self.ix_index,
            self.meta_pubkey,
            self.resolved_pubkey,
            self.static_contains_bad
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
        )
    }
}

/// Pre-sim audit: PumpSwap buy/sell ix meta #2 must resolve to the same pubkey in the compiled v0
/// message when lookups are resolved against the **on-chain** ALT snapshot.
pub fn audit_pump_amm_global_config_in_versioned_tx(
    message: &VersionedMessage,
    alt: &LoadedAlt,
    plan_instructions: &[Instruction],
) -> Result<(), PumpAmmGlobalConfigAuditError> {
    use crate::solana::dex::pumpfun_amm::pump_amm_canonical_global_config;

    let pump_program =
        Pubkey::from_str("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA").expect("pump amm program");
    let canonical_gc = pump_amm_canonical_global_config();
    let loaded = loaded_addresses_from_alt_lookups(alt, message);
    let static_keys: Vec<Pubkey> = match message {
        VersionedMessage::V0(v0) => v0.account_keys.clone(),
        VersionedMessage::Legacy(l) => l.account_keys.clone(),
    };
    let bad_static: Vec<Pubkey> = PUMP_AMM_KNOWN_BAD_GLOBAL_CONFIG_PUBKEYS
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .filter(|pk| static_keys.contains(pk))
        .collect();

    for (ix_index, plan_ix) in plan_instructions.iter().enumerate() {
        if plan_ix.program_id != pump_program || plan_ix.accounts.len() <= 2 {
            continue;
        }
        let meta_pk = plan_ix.accounts[2].pubkey;
        let Some(resolved_pk) =
            resolved_instruction_account_pubkey(message, loaded.as_ref(), ix_index, 2)
        else {
            return Err(PumpAmmGlobalConfigAuditError {
                ix_index,
                meta_pubkey: meta_pk,
                resolved_pubkey: Pubkey::default(),
                static_contains_bad: bad_static.clone(),
            });
        };
        if meta_pk != resolved_pk {
            return Err(PumpAmmGlobalConfigAuditError {
                ix_index,
                meta_pubkey: meta_pk,
                resolved_pubkey: resolved_pk,
                static_contains_bad: bad_static.clone(),
            });
        }
        if meta_pk == canonical_gc && !bad_static.is_empty() {
            return Err(PumpAmmGlobalConfigAuditError {
                ix_index,
                meta_pubkey: meta_pk,
                resolved_pubkey: resolved_pk,
                static_contains_bad: bad_static,
            });
        }
    }
    Ok(())
}

/// Exact 33-entry prod on-chain ALT snapshot (mainnet `2J74oVK…`, 2026-08-09).
pub fn prod_on_chain_alt_fixture() -> LoadedAlt {
    const ENTRIES: &[&str] = &[
        "11111111111111111111111111111111",
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
        "ComputeBudget111111111111111111111111111111",
        "SysvarRent111111111111111111111111111111111",
        "SysvarC1ock11111111111111111111111111111111",
        "So11111111111111111111111111111111111111112",
        "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
        "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
        "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
        "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        "D1ZN9Wj1fRSUQfCjhvnu1hqDMT7hzjzBBpi12nVniYD6",
        "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ",
        "C2aFPdENg4A2HQsmrd5rTw5TaYBX5Ku887cWjbFKtZpw",
        REMOVED_WRONG_PUMPSWAP_GLOBAL_CONFIG,
        "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf",
        "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM",
        "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h",
        "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
        "HFqU5x63VTqvQss8hp11i4bVmyBkEr7SrFKerWRx5Qdr",
        "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
        "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
        "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
        "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
        "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
        "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
        PUMPSWAP_AMM_GLOBAL_CONFIG,
        "5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx",
        PUMPSWAP_AMM_EVENT_AUTHORITY,
        PUMPFUN_BONDING_EVENT_AUTHORITY,
    ];
    LoadedAlt {
        address: Pubkey::from_str(PROD_ON_CHAIN_ALT_ADDRESS).expect("prod ALT address"),
        accounts: ENTRIES
            .iter()
            .map(|s| Pubkey::from_str(s).expect("prod ALT entry"))
            .collect(),
    }
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

    #[test]
    fn remap_v0_alt_lookup_indices_fixes_tip_filter_drift() {
        use solana_sdk::instruction::{AccountMeta, Instruction};
        use solana_system_program;

        let tip = Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5").unwrap();
        let global_config = Pubkey::from_str(PUMPSWAP_AMM_GLOBAL_CONFIG).unwrap();
        let alt = prod_on_chain_alt_fixture();
        assert_eq!(alt.index_of(&tip), Some(21));
        assert_eq!(alt.index_of(&global_config), Some(29));

        let payer = Pubkey::new_unique();
        let blockhash = solana_sdk::hash::Hash::new_unique();
        let mut data = vec![2, 0, 0, 0];
        data.extend_from_slice(&1u64.to_le_bytes());
        let ix = Instruction {
            program_id: solana_system_program::id(),
            accounts: vec![
                AccountMeta::new(payer, true),
                AccountMeta::new_readonly(global_config, false),
                AccountMeta::new(tip, false),
            ],
            data,
        };

        let without_remap = {
            let alt_account = address_lookup_table_account(&alt, Some(&tip));
            v0::Message::try_compile(&payer, std::slice::from_ref(&ix), &[alt_account], blockhash)
                .unwrap()
        };
        let mut with_remap = without_remap.clone();
        remap_v0_alt_lookup_indices_to_on_chain(&mut with_remap, &alt, &tip).unwrap();

        let loaded_before =
            loaded_addresses_from_alt_lookups(&alt, &VersionedMessage::V0(without_remap.clone()))
                .unwrap();
        let resolved_before = resolved_instruction_account_pubkey(
            &VersionedMessage::V0(without_remap),
            Some(&loaded_before),
            0,
            1,
        )
        .unwrap();
        assert_ne!(
            resolved_before, global_config,
            "unremapped filtered compile must drift vs on-chain ALT"
        );

        let loaded_after =
            loaded_addresses_from_alt_lookups(&alt, &VersionedMessage::V0(with_remap.clone()))
                .unwrap();
        let resolved_after = resolved_instruction_account_pubkey(
            &VersionedMessage::V0(with_remap),
            Some(&loaded_after),
            0,
            1,
        )
        .unwrap();
        assert_eq!(
            resolved_after, global_config,
            "remapped compile must resolve global_config against on-chain ALT"
        );
    }

    #[test]
    fn compile_v0_with_tip_filter_resolves_globals_against_on_chain_alt() {
        use solana_sdk::instruction::{AccountMeta, Instruction};
        use solana_system_program;

        let alt = prod_on_chain_alt_fixture();
        let tip = Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5").unwrap();
        let global_config = Pubkey::from_str(PUMPSWAP_AMM_GLOBAL_CONFIG).unwrap();
        let payer = Pubkey::new_unique();
        let blockhash = solana_sdk::hash::Hash::new_unique();
        let mut data = vec![2, 0, 0, 0];
        data.extend_from_slice(&1u64.to_le_bytes());
        let ix = Instruction {
            program_id: solana_system_program::id(),
            accounts: vec![
                AccountMeta::new(payer, true),
                AccountMeta::new_readonly(global_config, false),
                AccountMeta::new(tip, false),
            ],
            data,
        };
        let message = compile_v0_versioned_message(
            &payer,
            std::slice::from_ref(&ix),
            Some(&alt),
            Some(&tip),
            blockhash,
        )
        .expect("compile with remap");
        let loaded = loaded_addresses_from_alt_lookups(&alt, &message).unwrap();
        let resolved = resolved_instruction_account_pubkey(&message, Some(&loaded), 0, 1).unwrap();
        assert_eq!(resolved, global_config);
    }

    #[test]
    fn prod_on_chain_alt_fixture_layout() {
        let alt = prod_on_chain_alt_fixture();
        assert_eq!(alt.accounts.len(), 33);
        assert_eq!(
            alt.accounts[17].to_string(),
            REMOVED_WRONG_PUMPSWAP_GLOBAL_CONFIG
        );
        assert_eq!(alt.accounts[29].to_string(), PUMPSWAP_AMM_GLOBAL_CONFIG);
        assert!(!alt
            .accounts
            .iter()
            .any(|p| p.to_string() == PUMP_AMM_WRONG_LEGACY_GLOBAL_CONFIG));
    }
}
