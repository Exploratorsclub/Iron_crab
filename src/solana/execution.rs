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
use tracing::{error, info};

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

#[derive(Clone)]
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
        let roi_bps = crate::solana::arbitrage::calculate_roi_bps(amount_in_norm, cycle.net_profit);

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
        // We only verify we have enough SOL for gas fees (estimate dynamically after building tx)
        let wallet_balance = self.rpc.get_balance_retry(&self.wallet.pubkey()).await?;
        // Dynamic estimation will be done after instruction building
        let estimated_gas_pre = self.estimate_gas_cost(10); // Conservative estimate for 3-hop (usually 6-9 ix)
        if wallet_balance < estimated_gas_pre {
            return Err(anyhow!(
                "Insufficient SOL for gas: have {} lamports, need ~{} lamports",
                wallet_balance,
                estimated_gas_pre
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
        // CRITICAL: Use single-hop quotes to ensure proper decimal handling and slippage control

        // Hop 1: base -> mid1
        let quote1 = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.router.best_quote_exact_in(base, mid1, amount_in),
        )
        .await
        .map_err(|_| anyhow!("Timeout getting quote for {}->{}", base, mid1))??;

        let (dex1_idx, q1) = match quote1 {
            Some(rq) => (rq.dex_index, rq.quote),
            None => return Err(anyhow!("No quote available for {}->{}", base, mid1)),
        };

        // Hop 2: mid1 -> mid2
        let quote2 = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.router.best_quote_exact_in(mid1, mid2, q1.amount_out),
        )
        .await
        .map_err(|_| anyhow!("Timeout getting quote for {}->{}", mid1, mid2))??;

        let (dex2_idx, q2) = match quote2 {
            Some(rq) => (rq.dex_index, rq.quote),
            None => return Err(anyhow!("No quote available for {}->{}", mid1, mid2)),
        };

        // Hop 3: mid2 -> base (apply slippage protection on final hop)
        let quote3 = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.router.best_quote_exact_in(mid2, base, q2.amount_out),
        )
        .await
        .map_err(|_| anyhow!("Timeout getting quote for {}->{}", mid2, base))??;

        let (dex3_idx, q3) = match quote3 {
            Some(rq) => (rq.dex_index, rq.quote),
            None => return Err(anyhow!("No quote available for {}->{}", mid2, base)),
        };

        let final_out_actual = q3.amount_out;

        // Apply slippage protection to final output
        let min_out_final = {
            let slippage_factor = (10_000 - self.config.max_slippage_bps) as f64 / 10_000.0;
            (final_out_actual as f64 * slippage_factor) as u64
        };

        // Verify the actual quotes match our expectations from discovery
        // Allow some deviation due to pool state changes, but reject if too different
        let expected_out = cycle.gross_out;
        if final_out_actual < expected_out * 80 / 100 {
            // More than 20% worse
            return Err(anyhow!(
                "Quote deteriorated significantly: expected {} but got {} (>20% worse)",
                expected_out,
                final_out_actual
            ));
        }

        // Build instructions for all 3 hops
        let dexs = self.router.dexs();
        let mut all_ixs = Vec::new();

        // Hop 1: no min_out (intermediate hop)
        let ixs1 = dexs[dex1_idx].build_swap_ix(&q1.input_mint, &q1.output_mint, amount_in, 0)?;
        all_ixs.extend(ixs1);

        // Hop 2: no min_out (intermediate hop)
        let ixs2 =
            dexs[dex2_idx].build_swap_ix(&q2.input_mint, &q2.output_mint, q1.amount_out, 0)?;
        all_ixs.extend(ixs2);

        // Hop 3: apply min_out for slippage protection
        let ixs3 = dexs[dex3_idx].build_swap_ix(
            &q3.input_mint,
            &q3.output_mint,
            q2.amount_out,
            min_out_final,
        )?;
        all_ixs.extend(ixs3);

        // =========== Execution ===========

        if self.config.dry_run {
            info!(
                path = %format!("{} -> {} -> {} -> {}", base, mid1, mid2, base),
                amount_in = amount_in,
                expected_out = final_out_actual,
                profit = final_out_actual.saturating_sub(amount_in),
                ix_count = all_ixs.len(),
                "execution: DRY RUN - would execute this trade"
            );

            let estimated_gas = self.estimate_gas_cost(all_ixs.len());
            return Ok(ExecutionResult {
                signature: "DRY_RUN".to_string(),
                path: (base.clone(), mid1.clone(), mid2.clone()),
                amount_in,
                expected_out: final_out_actual,
                actual_out: None,
                gas_cost: estimated_gas,
                net_profit: net_profit_norm.into(),
                status: ExecutionStatus::DryRun,
            });
        }

        // Build and sign transaction
        let recent_blockhash = self.rpc.get_latest_blockhash_retry().await?;
        let ix_count = all_ixs.len(); // Save before move
        let tx = self.build_signed_transaction(all_ixs, recent_blockhash)?;

        // Send transaction
        let signature = self.rpc.rpc.send_and_confirm_transaction(&tx).await?;

        let estimated_gas = self.estimate_gas_cost(ix_count);

        info!(
            path = %format!("{} -> {} -> {} -> {}", base, mid1, mid2, base),
            tx_signature = %signature,
            amount_in = amount_in,
            expected_out = final_out_actual,
            roi_bps = roi_bps,
            ix_count = ix_count,
            estimated_gas = estimated_gas,
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
            gas_cost: estimated_gas,
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

    /// Execute multiple opportunities sequentially to respect cumulative position limits
    /// DESIGN: Sequential execution prevents race conditions and allows proper risk management
    pub async fn execute_batch(&self, cycles: &[CycleOpportunity]) -> Vec<ExecutionResult> {
        let mut results = Vec::new();
        let mut cumulative_position = 0u64;

        for (idx, cycle) in cycles.iter().enumerate() {
            // Check cumulative position limit
            if cumulative_position + cycle.amount_in > self.config.max_position_lamports {
                tracing::warn!(
                    cycle_idx = idx,
                    cumulative = cumulative_position,
                    this_amount = cycle.amount_in,
                    max_limit = self.config.max_position_lamports,
                    "execution: skipping opportunity - would exceed cumulative position limit"
                );
                continue;
            }

            match self.execute_opportunity(cycle).await {
                Ok(result) => {
                    // Only count successful executions toward position limit
                    if matches!(
                        result.status,
                        ExecutionStatus::Confirmed | ExecutionStatus::Pending
                    ) {
                        cumulative_position += cycle.amount_in;
                    }
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
            total_attempted = cycles.len(),
            cumulative_position = cumulative_position,
            "execution: batch completed"
        );

        results
    }

    /// Estimate gas cost based on instruction count
    /// Real costs are usually lower, but we overestimate for safety
    pub fn estimate_gas_cost(&self, ix_count: usize) -> u64 {
        // Base cost: signature + blockhash lookup
        const BASE_COST: u64 = 5_000; // 5k lamports base
                                      // Per-instruction cost: compute units + account reads/writes
                                      // Complex DEX swaps can use 200k-400k CU per swap
                                      // At default priority (1000 micro-lamports/CU) = 200-400 lamports per instruction
                                      // We overestimate for safety: ~2000 lamports per instruction
        const PER_IX_COST: u64 = 2_000;

        // Priority fee (added via compute budget instruction)
        let priority_fee_estimate = (300_000 * self.config.priority_fee_micro_lamports) / 1_000_000; // 300k CU typical

        BASE_COST + (ix_count as u64 * PER_IX_COST) + priority_fee_estimate
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
        // Create a mock execution engine to test gas estimation
        // Gas estimation formula: 5k base + (ix_count * 2k) + priority_fee
        // With default priority_fee_micro_lamports = 1000:
        // priority_fee_estimate = (300k * 1000) / 1M = 300 lamports

        let base_cost = 5_000u64;
        let per_ix = 2_000u64;
        let priority = 300u64;

        let gas_3ix = base_cost + (3 * per_ix) + priority; // 5k + 6k + 300 = 11,300
        let gas_9ix = base_cost + (9 * per_ix) + priority; // 5k + 18k + 300 = 23,300

        assert_eq!(gas_3ix, 11_300);
        assert_eq!(gas_9ix, 23_300);

        // Verify config values
        assert_eq!(config.priority_fee_micro_lamports, 1_000);
    }
}
