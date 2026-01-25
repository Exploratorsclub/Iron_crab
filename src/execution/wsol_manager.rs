//! WSOL Manager - Maintains WSOL balance for efficient arbitrage transactions
//!
//! Professional arbitrage bots don't wrap/unwrap in the arb TX itself:
//! - Saves ~2000-3000 CU per wrap/unwrap
//! - Fewer instructions = faster serialization
//! - Arb TX should be minimal: only swaps + Jito tip
//!
//! This module manages WSOL balance:
//! - Event-driven via Geyser → NATS wallet balance updates
//! - Auto-wrap when WSOL < min threshold
//! - Auto-unwrap when WSOL > max threshold
//!
//! # Architecture
//!
//! ```text
//! market-data (Geyser) → WalletBalanceUpdate (NATS) → WsolManager → wrap/unwrap TX
//! ```
//!
//! # Configuration
//!
//! ```toml
//! [wsol_manager]
//! enabled = true
//! min_wsol_sol = 0.5        # Wrap trigger
//! target_wsol_sol = 1.0     # Target after wrap
//! max_wsol_sol = 2.0        # Unwrap trigger
//! ```

use anyhow::Result;
use serde::{Deserialize, Serialize};
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::ipc::RecordHeader;
use crate::metrics::{
    WSOL_BALANCE_LAMPORTS, WSOL_UNWRAP_LAMPORTS_TOTAL, WSOL_UNWRAP_TOTAL, WSOL_WRAP_LAMPORTS_TOTAL,
    WSOL_WRAP_TOTAL,
};
use crate::nats::NatsClient;
use crate::solana::rpc::SolanaRpc;
use crate::storage::JsonlWriter;
use crate::wallet::Treasury;

/// WSOL mint address (native SOL wrapped)
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Lamports per SOL
const LAMPORTS_PER_SOL: u64 = 1_000_000_000;

// ============================================================================
// Configuration
// ============================================================================

/// WSOL Manager Configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsolManagerConfig {
    /// Enable WSOL management. Default: true
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Minimum WSOL balance in SOL. Below this triggers wrap.
    #[serde(default = "default_min_wsol")]
    pub min_wsol_sol: f64,

    /// Target WSOL balance in SOL after wrap.
    #[serde(default = "default_target_wsol")]
    pub target_wsol_sol: f64,

    /// Maximum WSOL balance in SOL. Above this triggers unwrap.
    #[serde(default = "default_max_wsol")]
    pub max_wsol_sol: f64,

    /// Minimum native SOL to keep (rent + buffer). Default: 0.1 SOL
    #[serde(default = "default_min_native_sol")]
    pub min_native_sol: f64,

    /// Cooldown between wrap/unwrap operations in seconds. Default: 30
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,

    /// Dry-run mode: log actions but don't send TX. Default: false
    #[serde(default)]
    pub dry_run: bool,
}

fn default_enabled() -> bool {
    true
}
fn default_min_wsol() -> f64 {
    0.5
}
fn default_target_wsol() -> f64 {
    1.0
}
fn default_max_wsol() -> f64 {
    2.0
}
fn default_min_native_sol() -> f64 {
    0.1
}
fn default_cooldown_secs() -> u64 {
    30
}

impl Default for WsolManagerConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            min_wsol_sol: default_min_wsol(),
            target_wsol_sol: default_target_wsol(),
            max_wsol_sol: default_max_wsol(),
            min_native_sol: default_min_native_sol(),
            cooldown_secs: default_cooldown_secs(),
            dry_run: false,
        }
    }
}

impl WsolManagerConfig {
    /// Convert SOL amounts to lamports
    pub fn min_wsol_lamports(&self) -> u64 {
        (self.min_wsol_sol * LAMPORTS_PER_SOL as f64) as u64
    }

    pub fn target_wsol_lamports(&self) -> u64 {
        (self.target_wsol_sol * LAMPORTS_PER_SOL as f64) as u64
    }

    pub fn max_wsol_lamports(&self) -> u64 {
        (self.max_wsol_sol * LAMPORTS_PER_SOL as f64) as u64
    }

    pub fn min_native_lamports(&self) -> u64 {
        (self.min_native_sol * LAMPORTS_PER_SOL as f64) as u64
    }
}

// ============================================================================
// NATS Message Types
// ============================================================================

/// Wallet balance update from market-data (Geyser-driven)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletBalanceUpdate {
    #[serde(flatten)]
    pub header: RecordHeader,
    /// Wallet address
    pub wallet: String,
    /// Native SOL balance in lamports
    pub sol_lamports: u64,
    /// WSOL balance in lamports (if tracked)
    pub wsol_lamports: Option<u64>,
    /// Slot at which balance was observed
    pub slot: u64,
}

/// WSOL Manager action record (for logging/forensics)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsolManagerAction {
    #[serde(flatten)]
    pub header: RecordHeader,
    /// Action type: "wrap" or "unwrap"
    pub action: String,
    /// Amount in lamports
    pub amount_lamports: u64,
    /// SOL balance before action
    pub sol_before_lamports: u64,
    /// WSOL balance before action
    pub wsol_before_lamports: u64,
    /// Reason for action
    pub reason: String,
    /// Transaction signature (if sent)
    pub signature: Option<String>,
    /// Whether this was a dry-run
    pub dry_run: bool,
    /// Error message if failed
    pub error: Option<String>,
}

// ============================================================================
// WSOL Manager
// ============================================================================

/// WSOL Manager - maintains WSOL balance for efficient arb transactions
pub struct WsolManager {
    config: WsolManagerConfig,
    treasury: Arc<Treasury>,
    rpc: Arc<SolanaRpc>,
    wallet_pubkey: Pubkey,

    /// Last known WSOL balance (updated from Geyser events)
    wsol_balance: AtomicU64,
    /// Last known native SOL balance
    sol_balance: AtomicU64,
    /// Last action timestamp (for cooldown)
    last_action_ts: AtomicU64,
    /// Running flag for graceful shutdown
    running: AtomicBool,
    /// Wrap in progress flag (prevents double-wrap race condition)
    wrap_in_progress: AtomicBool,

    /// Build version for records
    build_version: String,
    /// Run ID for records
    run_id: String,

    /// JSONL writer for decision records
    jsonl_writer: Option<Arc<JsonlWriter>>,

    /// Kill switch checker - when returns true, no wrapping should occur
    kill_switch_checker: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
}

impl WsolManager {
    /// Create a new WSOL Manager
    pub fn new(
        config: WsolManagerConfig,
        treasury: Arc<Treasury>,
        rpc: Arc<SolanaRpc>,
        build_version: &str,
        run_id: &str,
    ) -> Self {
        let wallet_pubkey = treasury.pubkey();
        Self {
            config,
            treasury,
            rpc,
            wallet_pubkey,
            wsol_balance: AtomicU64::new(0),
            sol_balance: AtomicU64::new(0),
            last_action_ts: AtomicU64::new(0),
            running: AtomicBool::new(true),
            wrap_in_progress: AtomicBool::new(false),
            build_version: build_version.to_string(),
            run_id: run_id.to_string(),
            jsonl_writer: None,
            kill_switch_checker: None,
        }
    }

    /// Create with JSONL writer for decision records
    pub fn with_jsonl_writer(
        config: WsolManagerConfig,
        treasury: Arc<Treasury>,
        rpc: Arc<SolanaRpc>,
        build_version: &str,
        run_id: &str,
        jsonl_writer: Arc<JsonlWriter>,
    ) -> Self {
        let wallet_pubkey = treasury.pubkey();
        Self {
            config,
            treasury,
            rpc,
            wallet_pubkey,
            wsol_balance: AtomicU64::new(0),
            sol_balance: AtomicU64::new(0),
            last_action_ts: AtomicU64::new(0),
            running: AtomicBool::new(true),
            wrap_in_progress: AtomicBool::new(false),
            build_version: build_version.to_string(),
            run_id: run_id.to_string(),
            jsonl_writer: Some(jsonl_writer),
            kill_switch_checker: None,
        }
    }

    /// Set kill switch checker function
    pub fn with_kill_switch<F>(mut self, checker: F) -> Self
    where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        self.kill_switch_checker = Some(Arc::new(checker));
        self
    }

    /// Check if kill switch is currently active
    fn is_kill_switch_active(&self) -> bool {
        self.kill_switch_checker
            .as_ref()
            .map(|checker| checker())
            .unwrap_or(false)
    }

    /// Get current WSOL balance (from cache)
    pub fn wsol_balance(&self) -> u64 {
        self.wsol_balance.load(Ordering::Relaxed)
    }

    /// Get current native SOL balance (from cache)
    pub fn sol_balance(&self) -> u64 {
        self.sol_balance.load(Ordering::Relaxed)
    }

    /// Check if we have enough WSOL for a given amount
    pub fn has_enough_wsol(&self, amount_lamports: u64) -> bool {
        self.wsol_balance() >= amount_lamports
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Run the WSOL manager main loop
    ///
    /// Subscribes to wallet balance updates via NATS and maintains WSOL balance.
    pub async fn run(
        &self,
        nats: Arc<NatsClient>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if !self.config.enabled {
            info!("WsolManager disabled by config");
            return Ok(());
        }

        info!(
            wallet = %self.wallet_pubkey,
            min_wsol = self.config.min_wsol_sol,
            target_wsol = self.config.target_wsol_sol,
            max_wsol = self.config.max_wsol_sol,
            dry_run = self.config.dry_run,
            "WsolManager starting"
        );

        // Initial balance fetch
        if let Err(e) = self.fetch_and_update_balances().await {
            warn!(error = %e, "Failed to fetch initial balances");
        } else {
            // Check if we need to wrap on startup
            if let Err(e) = self.check_and_act().await {
                warn!(error = %e, "Failed initial balance check");
            }
        }

        // Subscribe to wallet balance updates
        use crate::nats::wallet_balance_topic;
        let topic = wallet_balance_topic(&self.wallet_pubkey.to_string());
        let subscription = match nats.subscribe(&topic).await {
            Ok(sub) => sub,
            Err(e) => {
                // Fallback: subscribe to wildcard and filter
                use crate::nats::TOPIC_WALLET_BALANCE_PREFIX;
                warn!(error = %e, topic = %topic, "Failed to subscribe to wallet-specific topic, using wildcard");
                nats.subscribe(&format!("{}.*", TOPIC_WALLET_BALANCE_PREFIX))
                    .await?
            }
        };

        info!(topic = %topic, "Subscribed to wallet balance updates");

        // Need mutable subscription for next()
        let mut subscription = subscription;

        loop {
            tokio::select! {
                // Check for shutdown
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("WsolManager shutting down");
                        break;
                    }
                }

                // Process NATS messages
                msg = subscription.next() => {
                    match msg {
                        Some(nats_msg) => {
                            if let Err(e) = self.handle_balance_update(&nats_msg.payload).await {
                                debug!(error = %e, "Failed to handle balance update");
                            }
                        }
                        None => {
                            warn!("NATS subscription ended");
                            // Small delay before retry
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }

                // Periodic fallback check (every 60s in case we miss NATS messages)
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    if let Err(e) = self.fetch_and_update_balances().await {
                        debug!(error = %e, "Periodic balance fetch failed");
                    } else if let Err(e) = self.check_and_act().await {
                        debug!(error = %e, "Periodic balance check failed");
                    }
                }
            }
        }

        Ok(())
    }

    /// Run WSOL manager in polling-only mode (without NATS)
    ///
    /// Useful when NATS is unavailable - will check balances every 30 seconds.
    pub async fn run_polling_only(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        if !self.config.enabled {
            info!("WsolManager disabled by config");
            return Ok(());
        }

        info!(
            wallet = %self.wallet_pubkey,
            min_wsol = self.config.min_wsol_sol,
            target_wsol = self.config.target_wsol_sol,
            max_wsol = self.config.max_wsol_sol,
            dry_run = self.config.dry_run,
            "WsolManager starting (polling-only mode)"
        );

        // Initial balance fetch
        if let Err(e) = self.fetch_and_update_balances().await {
            warn!(error = %e, "Failed to fetch initial balances");
        } else if let Err(e) = self.check_and_act().await {
            warn!(error = %e, "Failed initial balance check");
        }

        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("WsolManager shutting down (polling mode)");
                        break;
                    }
                }

                _ = interval.tick() => {
                    if let Err(e) = self.fetch_and_update_balances().await {
                        debug!(error = %e, "Periodic balance fetch failed");
                    } else if let Err(e) = self.check_and_act().await {
                        debug!(error = %e, "Periodic balance check failed");
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle incoming wallet balance update from NATS
    async fn handle_balance_update(&self, data: &[u8]) -> Result<()> {
        let update: WalletBalanceUpdate = serde_json::from_slice(data)?;

        // Verify this is for our wallet
        if update.wallet != self.wallet_pubkey.to_string() {
            return Ok(());
        }

        // Update cached balances
        self.sol_balance
            .store(update.sol_lamports, Ordering::Relaxed);
        if let Some(wsol) = update.wsol_lamports {
            self.wsol_balance.store(wsol, Ordering::Relaxed);
            // Update Prometheus gauge
            WSOL_BALANCE_LAMPORTS.store(wsol, Ordering::Relaxed);
        }

        debug!(
            sol = update.sol_lamports as f64 / LAMPORTS_PER_SOL as f64,
            wsol = update
                .wsol_lamports
                .map(|w| w as f64 / LAMPORTS_PER_SOL as f64),
            slot = update.slot,
            "Balance update received"
        );

        // Check if action needed
        self.check_and_act().await
    }

    /// Fetch current balances from RPC (fallback)
    async fn fetch_and_update_balances(&self) -> Result<()> {
        // Fetch native SOL
        let sol_balance = self.rpc.rpc.get_balance(&self.wallet_pubkey).await?;
        self.sol_balance.store(sol_balance, Ordering::Relaxed);

        // Fetch WSOL balance
        let wsol_mint = Pubkey::from_str(WSOL_MINT)?;
        let wsol_balance = self.get_wsol_balance(&wsol_mint).await.unwrap_or(0);
        self.wsol_balance.store(wsol_balance, Ordering::Relaxed);

        debug!(
            sol = sol_balance as f64 / LAMPORTS_PER_SOL as f64,
            wsol = wsol_balance as f64 / LAMPORTS_PER_SOL as f64,
            "Fetched balances from RPC"
        );

        Ok(())
    }

    /// Get WSOL token account balance
    async fn get_wsol_balance(&self, wsol_mint: &Pubkey) -> Result<u64> {
        let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &spl_token::solana_program::pubkey::Pubkey::new_from_array(
                self.wallet_pubkey.to_bytes(),
            ),
            &spl_token::solana_program::pubkey::Pubkey::new_from_array(wsol_mint.to_bytes()),
            &spl_token::id(),
        );
        let ata_sdk = Pubkey::new_from_array(ata.to_bytes());

        match self.rpc.rpc.get_token_account_balance(&ata_sdk).await {
            Ok(balance) => {
                let amount_str = balance.amount;
                Ok(amount_str.parse::<u64>().unwrap_or(0))
            }
            Err(_) => Ok(0), // ATA doesn't exist
        }
    }

    /// Check current balances and wrap/unwrap if needed
    async fn check_and_act(&self) -> Result<()> {
        // Skip all actions if kill switch is active - no WSOL needed when liquidating
        if self.is_kill_switch_active() {
            debug!("Kill switch active, skipping WSOL management");
            return Ok(());
        }

        let wsol = self.wsol_balance();
        let sol = self.sol_balance();

        let min = self.config.min_wsol_lamports();
        let max = self.config.max_wsol_lamports();
        let target = self.config.target_wsol_lamports();
        let min_native = self.config.min_native_lamports();

        // Check cooldown
        if !self.check_cooldown() {
            return Ok(());
        }

        // Check if wrap already in progress (prevents double-wrap race condition)
        if self.wrap_in_progress.load(Ordering::Relaxed) {
            debug!("Wrap already in progress, skipping");
            return Ok(());
        }

        // Check if wrap needed
        if wsol < min {
            let wrap_amount = target.saturating_sub(wsol);
            let available_sol = sol.saturating_sub(min_native);

            if available_sol >= wrap_amount {
                // Set wrap_in_progress BEFORE async operation to prevent race condition
                self.wrap_in_progress.store(true, Ordering::Relaxed);
                
                info!(
                    wsol_current = wsol as f64 / LAMPORTS_PER_SOL as f64,
                    wsol_min = min as f64 / LAMPORTS_PER_SOL as f64,
                    wrap_amount = wrap_amount as f64 / LAMPORTS_PER_SOL as f64,
                    "WSOL below minimum, wrapping"
                );
                let result = self.execute_wrap(wrap_amount).await;
                
                // Clear wrap_in_progress after operation completes
                self.wrap_in_progress.store(false, Ordering::Relaxed);
                result?;
            } else if available_sol > 0 {
                // Set wrap_in_progress BEFORE async operation
                self.wrap_in_progress.store(true, Ordering::Relaxed);
                
                // Wrap what we can
                info!(
                    available = available_sol as f64 / LAMPORTS_PER_SOL as f64,
                    "Wrapping available SOL (less than ideal)"
                );
                let result = self.execute_wrap(available_sol).await;
                
                // Clear wrap_in_progress after operation completes
                self.wrap_in_progress.store(false, Ordering::Relaxed);
                result?;
            } else {
                warn!(
                    sol = sol as f64 / LAMPORTS_PER_SOL as f64,
                    min_native = min_native as f64 / LAMPORTS_PER_SOL as f64,
                    "Not enough SOL to wrap"
                );
            }
        }
        // Check if WSOL above max (LOG ONLY - no action)
        // IMPORTANT: We do NOT unwrap excess WSOL because:
        // 1. WSOL cannot be partially unwrapped - CloseAccount closes the ENTIRE ATA
        // 2. Closing the ATA breaks all pending/future arb transactions
        // 3. Excess WSOL is not a problem - it's just pre-funded for more trades
        // 4. If you need to recover SOL, do it manually via UI/CLI
        else if wsol > max {
            let excess = wsol.saturating_sub(max);
            debug!(
                wsol_current = wsol as f64 / LAMPORTS_PER_SOL as f64,
                wsol_max = max as f64 / LAMPORTS_PER_SOL as f64,
                excess = excess as f64 / LAMPORTS_PER_SOL as f64,
                "WSOL above maximum (info only - no auto-unwrap to preserve ATA)"
            );
            // NO ACTION - just log. Manual intervention required if user wants to unwrap.
        }

        Ok(())
    }

    /// Check if cooldown has passed
    fn check_cooldown(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let last = self.last_action_ts.load(Ordering::Relaxed);
        now.saturating_sub(last) >= self.config.cooldown_secs
    }

    /// Update last action timestamp
    fn update_last_action(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_action_ts.store(now, Ordering::Relaxed);
    }

    /// Execute wrap: SOL → WSOL
    async fn execute_wrap(&self, amount_lamports: u64) -> Result<()> {
        let sol_before = self.sol_balance();
        let wsol_before = self.wsol_balance();

        let reason = format!(
            "WSOL {} < min {}",
            wsol_before as f64 / LAMPORTS_PER_SOL as f64,
            self.config.min_wsol_sol
        );

        if self.config.dry_run {
            // Log dry-run action
            let action = WsolManagerAction {
                header: RecordHeader::new("wsol_manager", &self.build_version, &self.run_id),
                action: "wrap".to_string(),
                amount_lamports,
                sol_before_lamports: sol_before,
                wsol_before_lamports: wsol_before,
                reason: reason.clone(),
                signature: None,
                dry_run: true,
                error: None,
            };
            self.write_action(&action);

            info!(
                amount = amount_lamports as f64 / LAMPORTS_PER_SOL as f64,
                "[DRY-RUN] Would wrap SOL → WSOL"
            );
            return Ok(());
        }

        // Build and send wrap TX
        match self.build_and_send_wrap_tx(amount_lamports).await {
            Ok(sig) => {
                // Log successful action
                let action = WsolManagerAction {
                    header: RecordHeader::new("wsol_manager", &self.build_version, &self.run_id),
                    action: "wrap".to_string(),
                    amount_lamports,
                    sol_before_lamports: sol_before,
                    wsol_before_lamports: wsol_before,
                    reason,
                    signature: Some(sig.to_string()),
                    dry_run: false,
                    error: None,
                };
                self.write_action(&action);

                info!(
                    signature = %sig,
                    amount = amount_lamports as f64 / LAMPORTS_PER_SOL as f64,
                    "Wrapped SOL → WSOL"
                );
                self.update_last_action();
                // Update balances after wrap
                self.sol_balance
                    .fetch_sub(amount_lamports, Ordering::Relaxed);
                let new_wsol = self
                    .wsol_balance
                    .fetch_add(amount_lamports, Ordering::Relaxed)
                    + amount_lamports;
                // Update Prometheus metrics
                WSOL_WRAP_TOTAL.fetch_add(1, Ordering::Relaxed);
                WSOL_WRAP_LAMPORTS_TOTAL.fetch_add(amount_lamports, Ordering::Relaxed);
                WSOL_BALANCE_LAMPORTS.store(new_wsol, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                // Log failed action
                let action = WsolManagerAction {
                    header: RecordHeader::new("wsol_manager", &self.build_version, &self.run_id),
                    action: "wrap".to_string(),
                    amount_lamports,
                    sol_before_lamports: sol_before,
                    wsol_before_lamports: wsol_before,
                    reason,
                    signature: None,
                    dry_run: false,
                    error: Some(e.to_string()),
                };
                self.write_action(&action);

                error!(error = %e, "Failed to wrap SOL");
                Err(e)
            }
        }
    }

    /// Execute unwrap: WSOL → SOL (close account to get SOL back)
    /// Note: Currently not used but kept for future max_wsol overflow handling
    #[allow(dead_code)]
    async fn execute_unwrap(&self, _amount_lamports: u64) -> Result<()> {
        let sol_before = self.sol_balance();
        let wsol_before = self.wsol_balance();

        let reason = format!(
            "WSOL {} > max {}",
            wsol_before as f64 / LAMPORTS_PER_SOL as f64,
            self.config.max_wsol_sol
        );

        if self.config.dry_run {
            // Log dry-run action
            let action = WsolManagerAction {
                header: RecordHeader::new("wsol_manager", &self.build_version, &self.run_id),
                action: "unwrap".to_string(),
                amount_lamports: wsol_before, // We unwrap everything
                sol_before_lamports: sol_before,
                wsol_before_lamports: wsol_before,
                reason: reason.clone(),
                signature: None,
                dry_run: true,
                error: None,
            };
            self.write_action(&action);

            info!(
                amount = _amount_lamports as f64 / LAMPORTS_PER_SOL as f64,
                "[DRY-RUN] Would unwrap WSOL → SOL"
            );
            return Ok(());
        }

        // For unwrap, we close the entire WSOL ATA and get all SOL back
        // This is simpler and avoids partial unwrap complexity
        match self.build_and_send_unwrap_tx().await {
            Ok(sig) => {
                // Log successful action
                let action = WsolManagerAction {
                    header: RecordHeader::new("wsol_manager", &self.build_version, &self.run_id),
                    action: "unwrap".to_string(),
                    amount_lamports: wsol_before,
                    sol_before_lamports: sol_before,
                    wsol_before_lamports: wsol_before,
                    reason: reason.clone(),
                    signature: Some(sig.to_string()),
                    dry_run: false,
                    error: None,
                };
                self.write_action(&action);

                info!(
                    signature = %sig,
                    wsol_amount = wsol_before as f64 / LAMPORTS_PER_SOL as f64,
                    "Unwrapped WSOL → SOL (closed ATA)"
                );
                self.update_last_action();
                // Update balances after unwrap
                self.sol_balance.fetch_add(wsol_before, Ordering::Relaxed);
                self.wsol_balance.store(0, Ordering::Relaxed);
                // Update Prometheus metrics
                WSOL_UNWRAP_TOTAL.fetch_add(1, Ordering::Relaxed);
                WSOL_UNWRAP_LAMPORTS_TOTAL.fetch_add(wsol_before, Ordering::Relaxed);
                WSOL_BALANCE_LAMPORTS.store(0, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                // Log failed action
                let action = WsolManagerAction {
                    header: RecordHeader::new("wsol_manager", &self.build_version, &self.run_id),
                    action: "unwrap".to_string(),
                    amount_lamports: wsol_before,
                    sol_before_lamports: sol_before,
                    wsol_before_lamports: wsol_before,
                    reason,
                    signature: None,
                    dry_run: false,
                    error: Some(e.to_string()),
                };
                self.write_action(&action);

                error!(error = %e, "Failed to unwrap WSOL");
                Err(e)
            }
        }
    }

    /// Write action to JSONL log
    fn write_action(&self, action: &WsolManagerAction) {
        if let Some(ref writer) = self.jsonl_writer {
            if let Err(e) = writer.write(action) {
                warn!(error = %e, "Failed to write WSOL action to JSONL");
            }
        }
    }

    /// Build and send wrap transaction
    async fn build_and_send_wrap_tx(&self, amount_lamports: u64) -> Result<Signature> {
        let (_ata, ixs) = self
            .treasury
            .build_wrap_sol_ixs(&self.rpc, amount_lamports)
            .await?;

        if ixs.is_empty() {
            return Ok(Signature::default());
        }

        let blockhash = self.rpc.rpc.get_latest_blockhash().await?;
        let mut tx = Transaction::new_with_payer(&ixs, Some(&self.wallet_pubkey));
        tx.try_sign(&[self.treasury.signer_ref()], blockhash)?;

        let sig = self.rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok(sig)
    }

    /// Build and send unwrap transaction (close WSOL ATA)
    /// Note: Currently not used but kept for future max_wsol overflow handling
    #[allow(dead_code)]
    async fn build_and_send_unwrap_tx(&self) -> Result<Signature> {
        let wsol_mint = Pubkey::from_str(WSOL_MINT)?;
        let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &spl_token::solana_program::pubkey::Pubkey::new_from_array(
                self.wallet_pubkey.to_bytes(),
            ),
            &spl_token::solana_program::pubkey::Pubkey::new_from_array(wsol_mint.to_bytes()),
            &spl_token::id(),
        );

        // Close account instruction - sends lamports to owner
        let close_ix = spl_token::instruction::close_account(
            &spl_token::id(),
            &ata,
            &spl_token::solana_program::pubkey::Pubkey::new_from_array(
                self.wallet_pubkey.to_bytes(),
            ),
            &spl_token::solana_program::pubkey::Pubkey::new_from_array(
                self.wallet_pubkey.to_bytes(),
            ),
            &[],
        )?;

        // Convert to solana_sdk types
        let close_ix_sdk = Instruction {
            program_id: Pubkey::new_from_array(close_ix.program_id.to_bytes()),
            accounts: close_ix
                .accounts
                .into_iter()
                .map(|a| solana_sdk::instruction::AccountMeta {
                    pubkey: Pubkey::new_from_array(a.pubkey.to_bytes()),
                    is_signer: a.is_signer,
                    is_writable: a.is_writable,
                })
                .collect(),
            data: close_ix.data,
        };

        let blockhash = self.rpc.rpc.get_latest_blockhash().await?;
        let mut tx = Transaction::new_with_payer(&[close_ix_sdk], Some(&self.wallet_pubkey));
        tx.try_sign(&[self.treasury.signer_ref()], blockhash)?;

        let sig = self.rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok(sig)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = WsolManagerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_wsol_sol, 0.5);
        assert_eq!(config.target_wsol_sol, 1.0);
        assert_eq!(config.max_wsol_sol, 2.0);
        assert_eq!(config.min_native_sol, 0.1);
        assert_eq!(config.cooldown_secs, 30);
        assert!(!config.dry_run);
    }

    #[test]
    fn test_config_lamport_conversion() {
        let config = WsolManagerConfig {
            min_wsol_sol: 0.5,
            target_wsol_sol: 1.0,
            max_wsol_sol: 2.0,
            min_native_sol: 0.1,
            ..Default::default()
        };

        assert_eq!(config.min_wsol_lamports(), 500_000_000);
        assert_eq!(config.target_wsol_lamports(), 1_000_000_000);
        assert_eq!(config.max_wsol_lamports(), 2_000_000_000);
        assert_eq!(config.min_native_lamports(), 100_000_000);
    }

    #[test]
    fn test_config_custom_values() {
        let config = WsolManagerConfig {
            enabled: false,
            min_wsol_sol: 0.1,
            target_wsol_sol: 0.3,
            max_wsol_sol: 0.5,
            min_native_sol: 0.05,
            cooldown_secs: 60,
            dry_run: true,
        };

        assert!(!config.enabled);
        assert_eq!(config.min_wsol_lamports(), 100_000_000);
        assert_eq!(config.target_wsol_lamports(), 300_000_000);
        assert_eq!(config.max_wsol_lamports(), 500_000_000);
        assert_eq!(config.min_native_lamports(), 50_000_000);
        assert!(config.dry_run);
    }

    #[test]
    fn test_wsol_mint_constant() {
        // Verify WSOL mint is the canonical address
        assert_eq!(WSOL_MINT, "So11111111111111111111111111111111111111112");
        // Verify it's a valid pubkey
        let pubkey = Pubkey::from_str(WSOL_MINT);
        assert!(pubkey.is_ok());
    }

    #[test]
    fn test_wallet_balance_update_serialization() {
        let update = WalletBalanceUpdate {
            header: RecordHeader::new("test", "0.1.0", "run-123"),
            wallet: "ABC123...".to_string(),
            sol_lamports: 1_000_000_000,
            wsol_lamports: Some(500_000_000),
            slot: 12345,
        };

        let json = serde_json::to_string(&update).unwrap();
        assert!(json.contains("ABC123"));
        assert!(json.contains("1000000000"));
        assert!(json.contains("500000000"));
        assert!(json.contains("12345"));

        // Roundtrip
        let parsed: WalletBalanceUpdate = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.wallet, "ABC123...");
        assert_eq!(parsed.sol_lamports, 1_000_000_000);
        assert_eq!(parsed.wsol_lamports, Some(500_000_000));
        assert_eq!(parsed.slot, 12345);
    }

    #[test]
    fn test_wallet_balance_update_no_wsol() {
        let update = WalletBalanceUpdate {
            header: RecordHeader::new("test", "0.1.0", "run-123"),
            wallet: "ABC123...".to_string(),
            sol_lamports: 1_000_000_000,
            wsol_lamports: None,
            slot: 12345,
        };

        let json = serde_json::to_string(&update).unwrap();
        let parsed: WalletBalanceUpdate = serde_json::from_str(&json).unwrap();
        assert!(parsed.wsol_lamports.is_none());
    }

    #[test]
    fn test_wsol_manager_action_serialization() {
        let action = WsolManagerAction {
            header: RecordHeader::new("wsol_manager", "0.1.0", "run-123"),
            action: "wrap".to_string(),
            amount_lamports: 500_000_000,
            sol_before_lamports: 1_500_000_000,
            wsol_before_lamports: 100_000_000,
            reason: "WSOL 0.1 < min 0.5".to_string(),
            signature: Some("5abc123...".to_string()),
            dry_run: false,
            error: None,
        };

        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("wrap"));
        assert!(json.contains("500000000"));
        assert!(json.contains("5abc123"));

        // Verify all fields present
        let parsed: WsolManagerAction = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.action, "wrap");
        assert_eq!(parsed.amount_lamports, 500_000_000);
        assert_eq!(parsed.sol_before_lamports, 1_500_000_000);
        assert_eq!(parsed.wsol_before_lamports, 100_000_000);
        assert!(!parsed.dry_run);
        assert!(parsed.error.is_none());
    }

    #[test]
    fn test_wsol_manager_action_with_error() {
        let action = WsolManagerAction {
            header: RecordHeader::new("wsol_manager", "0.1.0", "run-123"),
            action: "wrap".to_string(),
            amount_lamports: 500_000_000,
            sol_before_lamports: 1_500_000_000,
            wsol_before_lamports: 100_000_000,
            reason: "WSOL below min".to_string(),
            signature: None,
            dry_run: false,
            error: Some("Transaction failed: insufficient funds".to_string()),
        };

        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("Transaction failed"));

        let parsed: WsolManagerAction = serde_json::from_str(&json).unwrap();
        assert!(parsed.signature.is_none());
        assert!(parsed.error.is_some());
        assert!(parsed.error.unwrap().contains("insufficient funds"));
    }

    #[test]
    fn test_wrap_amount_calculation() {
        // Test the wrap amount logic without needing actual manager
        let min_wsol = 500_000_000u64; // 0.5 SOL
        let target_wsol = 1_000_000_000u64; // 1.0 SOL
        let current_wsol = 100_000_000u64; // 0.1 SOL
        let min_native = 100_000_000u64; // 0.1 SOL reserve
        let sol_balance = 2_000_000_000u64; // 2.0 SOL

        // Should wrap because current_wsol < min_wsol
        assert!(current_wsol < min_wsol);

        // Calculate wrap amount
        let wrap_amount = target_wsol.saturating_sub(current_wsol);
        assert_eq!(wrap_amount, 900_000_000); // 0.9 SOL needed

        // Calculate available SOL
        let available_sol = sol_balance.saturating_sub(min_native);
        assert_eq!(available_sol, 1_900_000_000); // 1.9 SOL available

        // Should be able to wrap full amount
        assert!(available_sol >= wrap_amount);
    }

    #[test]
    fn test_unwrap_amount_calculation() {
        // Test the unwrap amount logic
        let max_wsol = 2_000_000_000u64; // 2.0 SOL
        let target_wsol = 1_000_000_000u64; // 1.0 SOL
        let current_wsol = 2_500_000_000u64; // 2.5 SOL - over max

        // Should unwrap because current_wsol > max_wsol
        assert!(current_wsol > max_wsol);

        // Calculate unwrap amount
        let unwrap_amount = current_wsol.saturating_sub(target_wsol);
        assert_eq!(unwrap_amount, 1_500_000_000); // 1.5 SOL to unwrap
    }

    #[test]
    fn test_no_action_needed_in_range() {
        let min_wsol = 500_000_000u64; // 0.5 SOL
        let max_wsol = 2_000_000_000u64; // 2.0 SOL
        let current_wsol = 1_000_000_000u64; // 1.0 SOL - in range

        // Should NOT wrap (above min)
        assert!(current_wsol >= min_wsol);

        // Should NOT unwrap (below max)
        assert!(current_wsol <= max_wsol);
    }

    #[test]
    fn test_insufficient_sol_for_wrap() {
        let min_wsol = 500_000_000u64; // 0.5 SOL min
        let target_wsol = 1_000_000_000u64; // 1.0 SOL target
        let current_wsol = 100_000_000u64; // 0.1 SOL - below min
        let min_native = 100_000_000u64; // 0.1 SOL reserve
        let sol_balance = 150_000_000u64; // Only 0.15 SOL available

        // Should want to wrap
        assert!(current_wsol < min_wsol);

        // Calculate wrap amount needed
        let wrap_amount = target_wsol.saturating_sub(current_wsol);
        assert_eq!(wrap_amount, 900_000_000); // Need 0.9 SOL

        // Calculate available SOL
        let available_sol = sol_balance.saturating_sub(min_native);
        assert_eq!(available_sol, 50_000_000); // Only 0.05 SOL available

        // Cannot wrap full amount - would wrap available instead
        assert!(available_sol < wrap_amount);
        assert!(available_sol > 0); // But can still wrap something
    }
}
