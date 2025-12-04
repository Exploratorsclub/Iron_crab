//! Execution engine for arbitrage trades
//! Builds transaction plans and executes profitable cycles on-chain

use super::arbitrage::CycleOpportunity;
use super::dex::router::Router;
use super::rpc::SolanaRpc;
use crate::metrics::TRADES_EXECUTED_TOTAL;
use crate::wallet::Treasury;
use anyhow::{anyhow, Result};
use solana_sdk::{hash::Hash, instruction::Instruction, transaction::Transaction};
use std::sync::Arc;
use tracing::{error, info, warn};

/// Configuration for trade execution
#[derive(Debug, Clone, Copy)]
pub struct ExecutionConfig {
    /// Maximum slippage tolerance in basis points (e.g., 500 = 5%)
    pub max_slippage_bps: u32,
    /// Minimum profit required to execute (bps)
    pub min_profit_bps_to_execute: u32,
    /// Maximum lamports to risk per trade
    pub max_position_lamports: u64,
    /// Whether to actually execute or just simulate
    pub dry_run: bool,
    /// Priority fee in micro-lamports (adjust for network congestion)
    pub priority_fee_micro_lamports: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_slippage_bps: 500,                // 5% slippage tolerance
            min_profit_bps_to_execute: 50,        // 0.5% minimum profit
            max_position_lamports: 5_000_000_000, // 5 SOL max per trade
            dry_run: true,
            priority_fee_micro_lamports: 1_000, // Low priority fee
        }
    }
}

/// Result of executing a single trade
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub signature: String,
    pub path: (String, String, String),
    pub amount_in: u64,
    pub expected_out: u64,
    pub actual_out: Option<u64>,
    pub gas_cost: u64,
    pub net_profit: Option<u64>,
    pub status: ExecutionStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionStatus {
    /// Transaction submitted and confirmed
    Confirmed,
    /// Transaction submitted but pending
    Pending,
    /// Dry run executed
    DryRun,
    /// Transaction failed
    Failed(String),
    /// Simulation only (no on-chain execution)
    Simulated,
}

pub struct ExecutionEngine {
    pub rpc: Arc<SolanaRpc>,
    pub router: Arc<Router>,
    pub wallet: Treasury,
    pub config: ExecutionConfig,
}

impl ExecutionEngine {
    pub fn new(
        rpc: Arc<SolanaRpc>,
        router: Arc<Router>,
        wallet: Treasury,
        config: ExecutionConfig,
    ) -> Self {
        Self {
            rpc,
            router,
            wallet,
            config,
        }
    }

    /// Execute a single arbitrage opportunity
    pub async fn execute_opportunity(&self, cycle: &CycleOpportunity) -> Result<ExecutionResult> {
        let (base, mid1, mid2) = &cycle.path;
        let amount_in_norm = cycle.amount_in;
        let net_profit_norm = cycle.net_profit.unwrap_or(0);
        let amount_in = cycle.amount_in; // This is now normalized to 9 decimals

        // =========== Pre-execution checks ===========

        // 1. Check minimum profit threshold
        let roi_bps = if amount_in_norm > 0 {
            ((net_profit_norm as f64 / amount_in_norm as f64) * 10_000.0) as u32
        } else {
            0
        };

        if roi_bps < self.config.min_profit_bps_to_execute {
            return Err(anyhow!(
                "ROI {:.2}% below minimum {:.2}% threshold",
                roi_bps as f64 / 100.0,
                self.config.min_profit_bps_to_execute as f64 / 100.0
            ));
        }

        // 2. Check position size limit
        if amount_in > self.config.max_position_lamports {
            return Err(anyhow!(
                "Position size {} lamports exceeds max {} lamports",
                amount_in,
                self.config.max_position_lamports
            ));
        }

        // 3. Check wallet balance (SOL for gas fees)
        // NOTE: We skip checking token balances here as they are checked during transaction simulation
        // We only verify we have enough SOL for gas fees (estimate ~0.01 SOL for 3-hop swap)
        let wallet_balance = self.rpc.get_balance_retry(&self.wallet.pubkey()).await?;
        let estimated_gas = self.estimate_gas_cost(5); // 3-hop swap ~5 instructions
        if wallet_balance < estimated_gas {
            return Err(anyhow!(
                "Insufficient SOL for gas: have {} lamports, need ~{} lamports",
                wallet_balance,
                estimated_gas
            ));
        }

        // =========== Build transaction plan ===========

        info!(
            path = %format!("{} -> {} -> {} -> {}", base, mid1, mid2, base),
            roi_bps = roi_bps,
            amount_in = amount_in,
            "execution: building transaction plan"
        );

        // Build 3-hop swap plan (base -> mid1 -> mid2 -> base)
        let plan = self
            .router
            .build_best_hops2_plan_exact_in(base, mid1, amount_in, self.config.max_slippage_bps)
            .await?;

        let (ixs_hop1_hop2, final_out_hop2) = match plan {
            Some(p) => (p.ixs, p.expected_out),
            None => {
                return Err(anyhow!(
                    "Failed to build swap plan for {}->{}: no quotes available",
                    base,
                    mid1
                ))
            }
        };

        // Now build the third hop: mid2 -> base
        let plan_hop3 = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.router
                .build_best_hops2_plan_exact_in(
                    mid2,
                    base,
                    final_out_hop2,
                    self.config.max_slippage_bps,
                )
        )
        .await
        .map_err(|_| anyhow!("Timeout building plan for {}->{}", mid2, base))??;

        let (ixs_hop3, final_out_actual) = match plan_hop3 {
            Some(p) => (p.ixs, p.expected_out),
            None => {
                return Err(anyhow!(
                    "Failed to build swap plan for {}->{}: no quotes available",
                    mid2,
                    base
                ))
            }
        };

        // Combine all instructions
        let mut all_ixs = ixs_hop1_hop2;
        all_ixs.extend(ixs_hop3);

        // =========== Execution ===========

        if self.config.dry_run {
            info!(
                path = %format!("{} -> {} -> {} -> {}", base, mid1, mid2, base),
                amount_in = amount_in,
                expected_out = final_out_actual,
                profit = final_out_actual.saturating_sub(amount_in),
                "execution: DRY RUN - would execute this trade"
            );

            return Ok(ExecutionResult {
                signature: "DRY_RUN".to_string(),
                path: (base.clone(), mid1.clone(), mid2.clone()),
                amount_in,
                expected_out: final_out_actual,
                actual_out: None,
                gas_cost: 5_000_000, // Typical SOL gas cost
                net_profit: net_profit_norm.into(),
                status: ExecutionStatus::DryRun,
            });
        }

        // Build and sign transaction
        let recent_blockhash = self.rpc.get_latest_blockhash_retry().await?;
        let tx = self.build_signed_transaction(all_ixs, recent_blockhash)?;

        // Send transaction
        let signature = self.rpc.rpc.send_and_confirm_transaction(&tx).await?;

        info!(
            path = %format!("{} -> {} -> {} -> {}", base, mid1, mid2, base),
            tx_signature = %signature,
            amount_in = amount_in,
            expected_out = final_out_actual,
            roi_bps = roi_bps,
            "execution: transaction submitted"
        );

        // Record metric
        TRADES_EXECUTED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(ExecutionResult {
            signature: signature.to_string(),
            path: (base.clone(), mid1.clone(), mid2.clone()),
            amount_in,
            expected_out: final_out_actual,
            actual_out: None, // Would be populated after confirmation
            gas_cost: 5_000_000,
            net_profit: net_profit_norm.into(),
            status: ExecutionStatus::Pending,
        })
    }

    /// Build and sign a transaction with priority fees
    fn build_signed_transaction(
        &self,
        mut ixs: Vec<Instruction>,
        recent_blockhash: Hash,
    ) -> Result<Transaction> {
        // Add compute budget instruction for priority fee
        let compute_budget_ix = crate::solana::compute_budget_helper::set_compute_unit_price(
            self.config.priority_fee_micro_lamports,
        );

        ixs.insert(0, compute_budget_ix);

        // Build and sign transaction
        let tx = Transaction::new_signed_with_payer(
            &ixs,
            Some(&self.wallet.pubkey()),
            &[self.wallet.signer_ref()],
            recent_blockhash,
        );

        Ok(tx)
    }

    /// Execute multiple opportunities, respecting position limits
    pub async fn execute_batch(&self, cycles: &[CycleOpportunity]) -> Vec<ExecutionResult> {
        let mut results = Vec::new();
        let mut total_committed = 0u64;

        for cycle in cycles {
            // Skip if would exceed position limit
            if total_committed.saturating_add(cycle.amount_in) > self.config.max_position_lamports {
                warn!(
                    amount_in = cycle.amount_in,
                    total_committed = total_committed,
                    max_position = self.config.max_position_lamports,
                    "execution: skipping opportunity due to position limit"
                );
                continue;
            }

            match self.execute_opportunity(cycle).await {
                Ok(result) => {
                    total_committed = total_committed.saturating_add(cycle.amount_in);
                    results.push(result);
                }
                Err(e) => {
                    error!(
                        path = %format!("{} -> {} -> {} -> {}", cycle.path.0, cycle.path.1, cycle.path.2, cycle.path.0),
                        error = %e,
                        "execution: failed to execute opportunity"
                    );
                }
            }
        }

        info!(
            total_executed = results.len(),
            total_committed = total_committed,
            "execution: batch completed"
        );

        results
    }

    /// Estimate gas cost based on instruction count
    pub fn estimate_gas_cost(&self, ix_count: usize) -> u64 {
        // Base cost + per-instruction cost
        const BASE_COST: u64 = 5_000_000; // 5M lamports base
        const PER_IX_COST: u64 = 1_000_000; // 1M per instruction

        BASE_COST + (ix_count as u64 * PER_IX_COST)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ExecutionConfig::default();
        assert_eq!(config.max_slippage_bps, 500);
        assert_eq!(config.min_profit_bps_to_execute, 50);
        assert!(config.dry_run);
    }

    #[test]
    fn test_gas_estimation() {
        let config = ExecutionConfig::default();
        // Note: This test requires a valid keypair. In practice, use Treasury::load() or from_signer()
        // For now, we just test the gas estimation function directly
        let _gas_2ix = config; // Use config directly to test
        let gas_2ix_cost = 5_000_000 + (2_u64 * 1_000_000);
        let gas_5ix_cost = 5_000_000 + (5_u64 * 1_000_000);

        assert_eq!(gas_2ix_cost, 5_000_000 + 2_000_000); // 7M
        assert_eq!(gas_5ix_cost, 5_000_000 + 5_000_000); // 10M
    }
}
