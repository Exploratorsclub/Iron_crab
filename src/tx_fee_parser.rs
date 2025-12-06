//! Transaction metadata parser for detailed fee breakdown.
//!
//! Extracts protocol fees, referrer fees, and compute overhead from transaction metadata.

use crate::types::{fee_vaults, FeeBreakdown};
use solana_client::rpc_response::{RpcConfirmedTransactionStatusWithSignature, UiTransactionEncoding};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionTokenBalance,
};
use std::str::FromStr;

/// Parse detailed fee breakdown from transaction metadata
pub fn parse_fee_breakdown(
    tx_meta: &EncodedConfirmedTransactionWithStatusMeta,
    treasury_owner: &Pubkey,
) -> anyhow::Result<FeeBreakdown> {
    let mut breakdown = FeeBreakdown::default();

    // 1. Extract base network fee from meta
    if let Some(meta) = &tx_meta.transaction.meta {
        breakdown.network_fee_lamports = meta.fee;

        // 2. Extract compute units consumed for overhead calculation
        if let Some(compute_units) = meta.compute_units_consumed {
            // Approximate compute overhead: compute_units * priority_fee_per_unit
            // Note: Priority fee is embedded in total fee, this is an approximation
            // Heuristic: ~5000 micro lamports per CU for typical priority
            let priority_fee_per_cu_micro = 5; // Conservative estimate
            breakdown.compute_overhead_sol_micro = compute_units.saturating_mul(priority_fee_per_cu_micro);
        }

        // 3. Parse token balance changes for protocol fee attribution
        if let (Some(pre_balances), Some(post_balances)) = (
            &meta.pre_token_balances,
            &meta.post_token_balances,
        ) {
            breakdown.protocol_fee_total_sol_micro += parse_protocol_fees_from_balances(
                pre_balances,
                post_balances,
                treasury_owner,
                &mut breakdown,
            )?;
        }
    }

    Ok(breakdown)
}

/// Parse protocol and referrer fees from token balance changes
fn parse_protocol_fees_from_balances(
    pre_balances: &[UiTransactionTokenBalance],
    post_balances: &[UiTransactionTokenBalance],
    treasury_owner: &Pubkey,
    breakdown: &mut FeeBreakdown,
) -> anyhow::Result<u64> {
    let mut total_protocol_fee_micro = 0u64;

    // Build index of post balances by account
    let mut post_by_account: std::collections::HashMap<usize, &UiTransactionTokenBalance> =
        std::collections::HashMap::new();
    for post in post_balances {
        post_by_account.insert(post.account_index as usize, post);
    }

    // Compare pre vs post for each account
    for pre in pre_balances {
        if let Some(post) = post_by_account.get(&(pre.account_index as usize)) {
            // Parse owner from pre/post if available
            if let (Some(pre_owner_str), Some(post_owner_str)) = (&pre.owner, &post.owner) {
                // Try to parse owner pubkeys
                if let (Ok(pre_owner), Ok(post_owner)) = (
                    Pubkey::from_str(pre_owner_str),
                    Pubkey::from_str(post_owner_str),
                ) {
                    // Skip treasury accounts (those are our trades)
                    if pre_owner == *treasury_owner || post_owner == *treasury_owner {
                        continue;
                    }

                    // Check if this is a known fee vault
                    let is_raydium = fee_vaults::is_raydium_fee_vault(&post_owner);
                    let is_orca = fee_vaults::is_orca_fee_vault(&post_owner);

                    // Calculate balance change (post - pre)
                    if let (Some(pre_amt), Some(post_amt)) =
                        (&pre.ui_token_amount.ui_amount, &post.ui_token_amount.ui_amount)
                    {
                        let delta = post_amt - pre_amt;
                        if delta > 0.0 {
                            // Positive delta = fee received by vault
                            // Convert to SOL micro (approximate: 1 token = 1 SOL for simplicity)
                            // In production, use proper token price oracle
                            let fee_micro = (delta * 1_000_000.0) as u64;

                            if is_raydium {
                                breakdown.raydium_protocol_fee_sol_micro =
                                    breakdown.raydium_protocol_fee_sol_micro.saturating_add(fee_micro);
                                total_protocol_fee_micro = total_protocol_fee_micro.saturating_add(fee_micro);
                            } else if is_orca {
                                breakdown.orca_protocol_fee_sol_micro =
                                    breakdown.orca_protocol_fee_sol_micro.saturating_add(fee_micro);
                                total_protocol_fee_micro = total_protocol_fee_micro.saturating_add(fee_micro);
                            } else {
                                // Unknown destination = potential referrer fee
                                breakdown.referrer_fee_sol_micro =
                                    breakdown.referrer_fee_sol_micro.saturating_add(fee_micro);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(total_protocol_fee_micro)
}

/// Helper to extract fee breakdown from transaction signature
pub async fn fetch_and_parse_fee_breakdown(
    rpc: &solana_client::rpc_client::RpcClient,
    signature: &solana_sdk::signature::Signature,
    treasury_owner: &Pubkey,
) -> anyhow::Result<FeeBreakdown> {
    // Fetch transaction with meta
    let tx = rpc
        .get_transaction(signature, UiTransactionEncoding::JsonParsed)
        .map_err(|e| anyhow::anyhow!("Failed to fetch transaction: {}", e))?;

    parse_fee_breakdown(&tx, treasury_owner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fee_breakdown_total() {
        let breakdown = FeeBreakdown {
            protocol_fee_total_sol_micro: 1000,
            raydium_protocol_fee_sol_micro: 600,
            orca_protocol_fee_sol_micro: 400,
            referrer_fee_sol_micro: 200,
            compute_overhead_sol_micro: 300,
            network_fee_lamports: 5000,
        };

        // Total = 1000 + 200 + 300 + (5000 * 1000) = 5001500 micro
        assert_eq!(breakdown.total_fees_sol_micro(), 5_001_500);
    }

    #[test]
    fn test_fee_vault_detection() {
        let raydium = fee_vaults::raydium_fee_owner();
        let orca = fee_vaults::orca_fee_owner();

        assert!(fee_vaults::is_raydium_fee_vault(&raydium));
        assert!(fee_vaults::is_orca_fee_vault(&orca));
        assert!(!fee_vaults::is_raydium_fee_vault(&orca));
        assert!(!fee_vaults::is_orca_fee_vault(&raydium));
    }
}
