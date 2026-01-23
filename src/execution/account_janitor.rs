//! Account Janitor - Background cleanup of dust and empty ATAs
//!
//! Responsibilities:
//! - Close empty ATAs to recover rent (~0.002 SOL each)
//! - Merge dust of same token into single ATA (future)
//! - Swap dust to SOL via Jupiter (future)
//!
//! This runs as a low-priority background task with configurable intervals.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use spl_token::solana_program::program_pack::Pack;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::ipc::RecordHeader;
use crate::metrics::{
    JANITOR_ACCOUNTS_SCANNED_TOTAL, JANITOR_CLOSE_ATA_TOTAL, JANITOR_SOL_RECOVERED_LAMPORTS,
    JANITOR_SWEEP_RUNS_TOTAL,
};
use crate::solana::rpc::SolanaRpc;
use crate::wallet::Treasury;

// ============================================================================
// Configuration
// ============================================================================

/// AccountJanitor configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccountJanitorConfig {
    /// Enable the janitor
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Interval for closing empty ATAs (seconds)
    #[serde(default = "default_close_ata_interval")]
    pub close_ata_interval_secs: u64,

    /// Minimum age of empty ATA before closing (seconds)
    /// Prevents closing ATAs that might be needed for in-flight transactions
    #[serde(default = "default_close_ata_min_age")]
    pub close_ata_min_age_secs: u64,

    /// Maximum ATAs to close per run (to avoid large transactions)
    #[serde(default = "default_close_ata_max_per_run")]
    pub close_ata_max_per_run: usize,

    /// Dry run mode - log actions but don't execute
    #[serde(default)]
    pub dry_run: bool,
    // Future: dust swap settings
    // pub swap_dust_interval_secs: u64,
    // pub swap_dust_min_value_sol: f64,
}

fn default_enabled() -> bool {
    false
}

fn default_close_ata_interval() -> u64 {
    3600 // 1 hour
}

fn default_close_ata_min_age() -> u64 {
    86400 // 24 hours
}

fn default_close_ata_max_per_run() -> usize {
    10
}

impl Default for AccountJanitorConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            close_ata_interval_secs: default_close_ata_interval(),
            close_ata_min_age_secs: default_close_ata_min_age(),
            close_ata_max_per_run: default_close_ata_max_per_run(),
            dry_run: false,
        }
    }
}

// ============================================================================
// Types
// ============================================================================

/// Information about an ATA
#[derive(Debug, Clone)]
pub struct AtaInfo {
    /// ATA address
    pub address: Pubkey,
    /// Token mint
    pub mint: Pubkey,
    /// Current balance (raw units)
    pub balance: u64,
    /// Token decimals
    pub decimals: u8,
    /// Estimated age in seconds (based on first transaction)
    pub estimated_age_secs: Option<u64>,
}

/// Janitor action record (for logging/forensics)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JanitorAction {
    #[serde(flatten)]
    pub header: RecordHeader,
    /// Action type: "close_ata", "merge_dust", "swap_dust"
    pub action: String,
    /// Number of accounts affected
    pub accounts_count: usize,
    /// SOL recovered (lamports)
    pub sol_recovered_lamports: u64,
    /// Transaction signature (if sent)
    pub signature: Option<String>,
    /// Whether this was a dry-run
    pub dry_run: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Details about affected accounts
    pub details: Vec<String>,
}

// ============================================================================
// AccountJanitor
// ============================================================================

/// Background task for cleaning up dust and empty accounts
pub struct AccountJanitor {
    config: AccountJanitorConfig,
    treasury: Arc<Treasury>,
    rpc: Arc<SolanaRpc>,
    wallet_pubkey: Pubkey,
    run_id: String,
}

impl AccountJanitor {
    /// Create a new AccountJanitor
    pub fn new(
        config: AccountJanitorConfig,
        treasury: Arc<Treasury>,
        rpc: Arc<SolanaRpc>,
        run_id: String,
    ) -> Self {
        let wallet_pubkey = treasury.pubkey();
        Self {
            config,
            treasury,
            rpc,
            wallet_pubkey,
            run_id,
        }
    }

    /// Run the janitor main loop
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        if !self.config.enabled {
            info!("AccountJanitor disabled by config");
            return Ok(());
        }

        info!(
            wallet = %self.wallet_pubkey,
            close_interval_secs = self.config.close_ata_interval_secs,
            min_age_secs = self.config.close_ata_min_age_secs,
            max_per_run = self.config.close_ata_max_per_run,
            dry_run = self.config.dry_run,
            "AccountJanitor starting"
        );

        let mut close_interval =
            tokio::time::interval(Duration::from_secs(self.config.close_ata_interval_secs));

        // Skip first tick (immediate)
        close_interval.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("AccountJanitor shutting down");
                        break;
                    }
                }

                _ = close_interval.tick() => {
                    if let Err(e) = self.run_close_empty_atas().await {
                        warn!(error = %e, "Failed to close empty ATAs");
                    }
                }
            }
        }

        Ok(())
    }

    /// Find and close empty ATAs
    async fn run_close_empty_atas(&self) -> Result<()> {
        info!("AccountJanitor: Starting empty ATA scan");
        // Update sweep run counter
        JANITOR_SWEEP_RUNS_TOTAL.fetch_add(1, Ordering::Relaxed);

        // Find empty ATAs
        let empty_atas = self.find_empty_atas().await?;

        if empty_atas.is_empty() {
            debug!("No empty ATAs found");
            return Ok(());
        }

        info!(
            count = empty_atas.len(),
            "Found empty ATAs to potentially close"
        );

        // Filter by age (skip recently created ATAs)
        let old_enough: Vec<_> = empty_atas
            .into_iter()
            .filter(|ata| {
                ata.estimated_age_secs
                    .map(|age| age >= self.config.close_ata_min_age_secs)
                    .unwrap_or(false) // Skip if age unknown
            })
            .take(self.config.close_ata_max_per_run)
            .collect();

        if old_enough.is_empty() {
            debug!("No empty ATAs old enough to close");
            return Ok(());
        }

        info!(
            count = old_enough.len(),
            "Empty ATAs meeting age requirement"
        );

        // Close ATAs
        let result = self.close_atas(&old_enough).await;

        // Log action
        let action = JanitorAction {
            header: RecordHeader::new("account-janitor", env!("CARGO_PKG_VERSION"), &self.run_id),
            action: "close_ata".to_string(),
            accounts_count: old_enough.len(),
            sol_recovered_lamports: old_enough.len() as u64 * 2_039_280, // ~0.00203928 SOL per ATA
            signature: result.as_ref().ok().map(|s| s.to_string()),
            dry_run: self.config.dry_run,
            error: result.as_ref().err().map(|e| e.to_string()),
            details: old_enough
                .iter()
                .map(|ata| format!("{}:{}", ata.address, ata.mint))
                .collect(),
        };

        if self.config.dry_run {
            info!(
                action = ?action,
                "DRY RUN: Would close ATAs"
            );
        } else {
            match &result {
                Ok(sig) => {
                    // Update Prometheus metrics on success
                    let sol_recovered = old_enough.len() as u64 * 2_039_280;
                    JANITOR_CLOSE_ATA_TOTAL.fetch_add(old_enough.len() as u64, Ordering::Relaxed);
                    JANITOR_SOL_RECOVERED_LAMPORTS.fetch_add(sol_recovered, Ordering::Relaxed);

                    info!(
                        signature = %sig,
                        count = old_enough.len(),
                        sol_recovered = sol_recovered as f64 / 1e9,
                        "Successfully closed empty ATAs"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "Failed to close ATAs");
                }
            }
        }

        result.map(|_| ())
    }

    /// Find all empty ATAs for the wallet
    async fn find_empty_atas(&self) -> Result<Vec<AtaInfo>> {
        // Get all token accounts for wallet
        let token_accounts = self
            .rpc
            .get_token_accounts_by_owner(&self.wallet_pubkey)
            .await?;

        // Update scanned accounts metric
        JANITOR_ACCOUNTS_SCANNED_TOTAL.fetch_add(token_accounts.len() as u64, Ordering::Relaxed);

        let mut empty_atas = Vec::new();

        for (address, account) in token_accounts {
            // Parse token account
            if let Ok(token_account) = spl_token::state::Account::unpack(&account.data) {
                if token_account.amount == 0 {
                    // Get ATA age estimate
                    let age = self.estimate_ata_age(&address).await.ok();

                    empty_atas.push(AtaInfo {
                        address,
                        mint: Pubkey::new_from_array(token_account.mint.to_bytes()),
                        balance: 0,
                        decimals: 0, // Not needed for closing
                        estimated_age_secs: age,
                    });
                }
            }
        }

        Ok(empty_atas)
    }

    /// Estimate the age of an ATA based on first transaction
    async fn estimate_ata_age(&self, address: &Pubkey) -> Result<u64> {
        // Get first signature for this account
        let signatures = self
            .rpc
            .get_signatures_for_address(address, Some(1))
            .await?;

        if let Some(sig_info) = signatures.first() {
            if let Some(block_time) = sig_info.block_time {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                return Ok((now - block_time).max(0) as u64);
            }
        }

        Err(anyhow!("Could not determine ATA age"))
    }

    /// Close multiple ATAs in a single transaction
    async fn close_atas(&self, atas: &[AtaInfo]) -> Result<Signature> {
        if self.config.dry_run {
            return Ok(Signature::default());
        }

        if atas.is_empty() {
            return Err(anyhow!("No ATAs to close"));
        }

        let wallet_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(
            self.wallet_pubkey.to_bytes(),
        );

        let mut instructions = Vec::with_capacity(atas.len());

        for ata in atas {
            let ata_spl =
                spl_token::solana_program::pubkey::Pubkey::new_from_array(ata.address.to_bytes());

            // Determine token program (SPL Token vs Token-2022)
            // For now, assume SPL Token. TODO: Check account owner for Token-2022
            let token_program = spl_token::id();

            let close_ix = spl_token::instruction::close_account(
                &token_program,
                &ata_spl,    // account to close
                &wallet_spl, // destination for rent
                &wallet_spl, // owner/authority
                &[],         // no multisig
            )?;

            instructions.push(prog_ix_to_sdk(close_ix));
        }

        // Build and send transaction
        let recent_blockhash = self.rpc.get_latest_blockhash_retry().await?;
        let signer = self.treasury.signer_ref();

        let tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet_pubkey),
            &[signer],
            recent_blockhash,
        );

        let signature = self.rpc.send_and_confirm_transaction(&tx).await?;

        Ok(signature)
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Convert spl_token instruction to solana_sdk instruction
fn prog_ix_to_sdk(ix: spl_token::solana_program::instruction::Instruction) -> Instruction {
    Instruction {
        program_id: Pubkey::new_from_array(ix.program_id.to_bytes()),
        accounts: ix
            .accounts
            .into_iter()
            .map(|a| solana_sdk::instruction::AccountMeta {
                pubkey: Pubkey::new_from_array(a.pubkey.to_bytes()),
                is_signer: a.is_signer,
                is_writable: a.is_writable,
            })
            .collect(),
        data: ix.data,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AccountJanitorConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.close_ata_interval_secs, 3600);
        assert_eq!(config.close_ata_min_age_secs, 86400);
        assert_eq!(config.close_ata_max_per_run, 10);
        assert!(!config.dry_run);
    }

    #[test]
    fn test_config_serde_defaults() {
        // Test that TOML deserialization works with minimal config
        let toml_str = r#"
            enabled = true
        "#;
        let config: AccountJanitorConfig = toml::from_str(toml_str).unwrap();
        assert!(config.enabled);
        assert_eq!(config.close_ata_interval_secs, 3600); // default
        assert_eq!(config.close_ata_min_age_secs, 86400); // default
        assert_eq!(config.close_ata_max_per_run, 10); // default
    }

    #[test]
    fn test_config_serde_full() {
        let toml_str = r#"
            enabled = true
            close_ata_interval_secs = 7200
            close_ata_min_age_secs = 43200
            close_ata_max_per_run = 5
            dry_run = true
        "#;
        let config: AccountJanitorConfig = toml::from_str(toml_str).unwrap();
        assert!(config.enabled);
        assert_eq!(config.close_ata_interval_secs, 7200);
        assert_eq!(config.close_ata_min_age_secs, 43200);
        assert_eq!(config.close_ata_max_per_run, 5);
        assert!(config.dry_run);
    }

    #[test]
    fn test_ata_info_struct() {
        let ata = AtaInfo {
            address: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            balance: 0,
            decimals: 9,
            estimated_age_secs: Some(100000),
        };
        assert_eq!(ata.balance, 0);
        assert_eq!(ata.decimals, 9);
        assert!(ata.estimated_age_secs.is_some());
    }

    #[test]
    fn test_janitor_action_serialization() {
        use crate::ipc::RecordHeader;

        let action = JanitorAction {
            header: RecordHeader::new("account_janitor", "test-build", "test-run"),
            action: "close_ata".to_string(),
            accounts_count: 3,
            sol_recovered_lamports: 6117840, // ~0.006 SOL for 3 ATAs
            signature: Some("5abc123...".to_string()),
            dry_run: false,
            error: None,
            details: vec![
                "Closed ATA: ABC...".to_string(),
                "Closed ATA: DEF...".to_string(),
            ],
        };

        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("close_ata"));
        assert!(json.contains("6117840"));
        assert!(json.contains("accounts_count"));
    }

    #[test]
    fn test_prog_ix_to_sdk_conversion() {
        use spl_token::solana_program::instruction::AccountMeta as SplAccountMeta;
        use spl_token::solana_program::instruction::Instruction as SplInstruction;
        use spl_token::solana_program::pubkey::Pubkey as SplPubkey;

        let spl_ix = SplInstruction {
            program_id: SplPubkey::new_unique(),
            accounts: vec![
                SplAccountMeta {
                    pubkey: SplPubkey::new_unique(),
                    is_signer: true,
                    is_writable: true,
                },
                SplAccountMeta {
                    pubkey: SplPubkey::new_unique(),
                    is_signer: false,
                    is_writable: false,
                },
            ],
            data: vec![1, 2, 3, 4],
        };

        let sdk_ix = prog_ix_to_sdk(spl_ix.clone());

        assert_eq!(sdk_ix.accounts.len(), 2);
        assert!(sdk_ix.accounts[0].is_signer);
        assert!(sdk_ix.accounts[0].is_writable);
        assert!(!sdk_ix.accounts[1].is_signer);
        assert!(!sdk_ix.accounts[1].is_writable);
        assert_eq!(sdk_ix.data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_ata_info_no_age() {
        let ata = AtaInfo {
            address: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            balance: 0,
            decimals: 6,
            estimated_age_secs: None,
        };

        // ATA without age should not be selected for closing
        assert!(ata.estimated_age_secs.is_none());
        assert_eq!(ata.balance, 0);
    }

    #[test]
    fn test_ata_age_filter_logic() {
        // Test the age filtering logic
        let min_age_secs: u64 = 86400; // 24 hours

        let young_ata = AtaInfo {
            address: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            balance: 0,
            decimals: 9,
            estimated_age_secs: Some(3600), // 1 hour - too young
        };

        let old_ata = AtaInfo {
            address: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            balance: 0,
            decimals: 9,
            estimated_age_secs: Some(172800), // 48 hours - old enough
        };

        let unknown_age_ata = AtaInfo {
            address: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            balance: 0,
            decimals: 9,
            estimated_age_secs: None,
        };

        // Check filtering logic
        let young_passes = young_ata
            .estimated_age_secs
            .map(|age| age >= min_age_secs)
            .unwrap_or(false);
        let old_passes = old_ata
            .estimated_age_secs
            .map(|age| age >= min_age_secs)
            .unwrap_or(false);
        let unknown_passes = unknown_age_ata
            .estimated_age_secs
            .map(|age| age >= min_age_secs)
            .unwrap_or(false);

        assert!(!young_passes, "Young ATA should not pass filter");
        assert!(old_passes, "Old ATA should pass filter");
        assert!(!unknown_passes, "Unknown age ATA should not pass filter");
    }

    #[test]
    fn test_sol_recovery_calculation() {
        // Each ATA costs ~0.00203928 SOL in rent
        const RENT_PER_ATA: u64 = 2_039_280;

        let atas_to_close = 5;
        let expected_recovery = atas_to_close as u64 * RENT_PER_ATA;

        assert_eq!(expected_recovery, 10_196_400);
        assert!(expected_recovery as f64 / 1e9 > 0.01); // > 0.01 SOL
    }

    #[test]
    fn test_max_per_run_limit() {
        let config = AccountJanitorConfig {
            enabled: true,
            close_ata_max_per_run: 3,
            ..Default::default()
        };

        // Simulate having 10 ATAs
        let all_atas: Vec<AtaInfo> = (0..10)
            .map(|_| AtaInfo {
                address: Pubkey::new_unique(),
                mint: Pubkey::new_unique(),
                balance: 0,
                decimals: 9,
                estimated_age_secs: Some(100000),
            })
            .collect();

        // Take only max_per_run
        let to_close: Vec<_> = all_atas
            .into_iter()
            .take(config.close_ata_max_per_run)
            .collect();

        assert_eq!(to_close.len(), 3);
    }

    #[test]
    fn test_janitor_action_dry_run() {
        use crate::ipc::RecordHeader;

        let action = JanitorAction {
            header: RecordHeader::new("account_janitor", "test-build", "test-run"),
            action: "close_ata".to_string(),
            accounts_count: 2,
            sol_recovered_lamports: 4_078_560,
            signature: None, // No signature in dry-run
            dry_run: true,
            error: None,
            details: vec!["Would close ATA: ABC...".to_string()],
        };

        assert!(action.dry_run);
        assert!(action.signature.is_none());

        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("dry_run\":true"));
    }

    #[test]
    fn test_janitor_action_with_failure() {
        use crate::ipc::RecordHeader;

        let action = JanitorAction {
            header: RecordHeader::new("account_janitor", "test-build", "test-run"),
            action: "close_ata".to_string(),
            accounts_count: 2,
            sol_recovered_lamports: 0,
            signature: None,
            dry_run: false,
            error: Some("Transaction simulation failed".to_string()),
            details: vec![],
        };

        assert!(action.error.is_some());
        assert_eq!(action.sol_recovered_lamports, 0);

        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("simulation failed"));
    }
}
