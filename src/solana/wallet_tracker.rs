//! Wallet Tracker Module
//!
//! Tracks known wallets (smart money, bad actors, dev wallets) and early buyers.
//! Emits WalletActivity and EarlyBuyerDetected events when tracked wallets are active.
//!
//! Memory-bounded: Uses LRU cache for wallet states, configurable max size.

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

use crate::config::WalletTrackerCfg;
use crate::ipc::{
    AlertSeverity, InsiderAlertType, MarketEvent, MarketEventKind, RecordHeader, WalletAction,
    WalletType,
};

/// Maximum number of tokens to track early buyers for (LRU eviction)
const MAX_TRACKED_TOKENS: usize = 500;

/// Wallet tracker state
pub struct WalletTracker {
    config: WalletTrackerCfg,

    /// Known smart money wallets (fast lookup)
    smart_money: HashSet<String>,

    /// Known bad actor wallets
    bad_actors: HashSet<String>,

    /// Dev wallets per token (mint -> dev_wallet)
    dev_wallets: RwLock<HashMap<String, String>>,

    /// Early buyers per token (mint -> Vec<(wallet, slot, rank)>)
    early_buyers: RwLock<HashMap<String, Vec<EarlyBuyerInfo>>>,

    /// Pool creation slots (mint -> creation_slot)
    pool_creation_slots: RwLock<HashMap<String, u64>>,

    /// Token tracking order for LRU eviction
    token_tracking_order: RwLock<Vec<String>>,

    /// Win rates for known wallets (wallet -> win_rate)
    wallet_win_rates: RwLock<HashMap<String, f64>>,
}

#[derive(Debug, Clone)]
struct EarlyBuyerInfo {
    wallet: String,
    slot: u64,
    rank: u32,
    amount_sol: u64,
    amount_tokens: u64,
}

impl WalletTracker {
    /// Create a new wallet tracker from config
    pub fn new(config: WalletTrackerCfg) -> Self {
        let smart_money: HashSet<String> = config.smart_money_wallets.iter().cloned().collect();
        let bad_actors: HashSet<String> = config.bad_actor_wallets.iter().cloned().collect();

        info!(
            smart_money_count = smart_money.len(),
            bad_actor_count = bad_actors.len(),
            early_buyer_slots = config.early_buyer_slots,
            max_early_buyers = config.max_early_buyers_per_token,
            "Wallet tracker initialized"
        );

        Self {
            config,
            smart_money,
            bad_actors,
            dev_wallets: RwLock::new(HashMap::new()),
            early_buyers: RwLock::new(HashMap::new()),
            pool_creation_slots: RwLock::new(HashMap::new()),
            token_tracking_order: RwLock::new(Vec::new()),
            wallet_win_rates: RwLock::new(HashMap::new()),
        }
    }

    /// Create with default config
    pub fn default() -> Self {
        Self::new(WalletTrackerCfg::default())
    }

    /// Record a pool creation to start tracking early buyers
    pub fn record_pool_created(&self, mint: &str, slot: u64) {
        let mut slots = self.pool_creation_slots.write();
        let mut order = self.token_tracking_order.write();
        let mut early_buyers = self.early_buyers.write();

        // LRU eviction if at capacity
        if slots.len() >= MAX_TRACKED_TOKENS {
            if let Some(oldest) = order.first().cloned() {
                slots.remove(&oldest);
                early_buyers.remove(&oldest);
                order.remove(0);
                debug!(mint = %oldest, "Evicted oldest token from wallet tracker");
            }
        }

        slots.insert(mint.to_string(), slot);
        early_buyers.insert(mint.to_string(), Vec::new());
        order.push(mint.to_string());

        debug!(mint = %mint, slot = slot, "Recording pool creation for early buyer tracking");
    }

    /// Record a dev wallet for a token
    pub fn record_dev_wallet(&self, mint: &str, dev_wallet: &str) {
        let mut devs = self.dev_wallets.write();
        devs.insert(mint.to_string(), dev_wallet.to_string());
        debug!(mint = %mint, dev = %dev_wallet, "Recorded dev wallet");
    }

    /// Process a trade and check if it involves a tracked wallet
    /// Returns events to emit (if any)
    pub fn process_trade(
        &self,
        mint: &str,
        trader_wallet: &str,
        is_buy: bool,
        amount_sol: u64,
        amount_tokens: u64,
        slot: u64,
        signature: &str,
        run_id: &str,
        component: &str,
    ) -> Vec<MarketEvent> {
        let mut events = Vec::new();

        // Check if this is a known wallet
        let wallet_type = self.classify_wallet(trader_wallet, mint);

        if let Some(ref wtype) = wallet_type {
            let action = if is_buy {
                WalletAction::Buy
            } else {
                WalletAction::Sell
            };
            let win_rate = self.wallet_win_rates.read().get(trader_wallet).copied();

            let event = MarketEvent {
                header: RecordHeader::new(component, env!("CARGO_PKG_VERSION"), run_id),
                event_id: format!("wallet-{}", &signature[..16]),
                source: "wallet-tracker".to_string(),
                slot: Some(slot),
                kind: MarketEventKind::WalletActivity {
                    wallet: trader_wallet.to_string(),
                    wallet_type: wtype.clone(),
                    action: action.clone(),
                    mint: mint.to_string(),
                    amount_sol,
                    amount_tokens,
                    signature: signature.to_string(),
                    wallet_win_rate: win_rate,
                },
            };
            events.push(event);

            // Check for insider alerts
            if let Some(alert) = self.check_insider_alert(
                wtype,
                &action,
                mint,
                trader_wallet,
                amount_sol,
                run_id,
                component,
            ) {
                events.push(alert);
            }
        }

        // Check for early buyer (only for buys)
        if is_buy {
            if let Some(event) = self.check_early_buyer(
                mint,
                trader_wallet,
                slot,
                amount_sol,
                amount_tokens,
                run_id,
                component,
            ) {
                events.push(event);
            }
        }

        // Check for whale activity
        if amount_sol >= self.config.whale_threshold_lamports && wallet_type.is_none() {
            let action = if is_buy {
                WalletAction::Buy
            } else {
                WalletAction::Sell
            };
            let event = MarketEvent {
                header: RecordHeader::new(component, env!("CARGO_PKG_VERSION"), run_id),
                event_id: format!("whale-{}", &signature[..16]),
                source: "wallet-tracker".to_string(),
                slot: Some(slot),
                kind: MarketEventKind::WalletActivity {
                    wallet: trader_wallet.to_string(),
                    wallet_type: WalletType::Whale,
                    action,
                    mint: mint.to_string(),
                    amount_sol,
                    amount_tokens,
                    signature: signature.to_string(),
                    wallet_win_rate: None,
                },
            };
            events.push(event);
        }

        events
    }

    /// Classify a wallet into a type (if known)
    fn classify_wallet(&self, wallet: &str, mint: &str) -> Option<WalletType> {
        // Check smart money first (highest priority)
        if self.smart_money.contains(wallet) {
            return Some(WalletType::SmartMoney);
        }

        // Check bad actors
        if self.bad_actors.contains(wallet) {
            return Some(WalletType::KnownBadActor);
        }

        // Check if dev wallet for this token
        let devs = self.dev_wallets.read();
        if devs.get(mint).map(|d| d == wallet).unwrap_or(false) {
            return Some(WalletType::DevWallet);
        }

        // Check if known early buyer for this token
        let early = self.early_buyers.read();
        if let Some(buyers) = early.get(mint) {
            if buyers.iter().any(|b| b.wallet == wallet) {
                return Some(WalletType::EarlyBuyer);
            }
        }

        None
    }

    /// Check if this is an early buyer and record if so
    fn check_early_buyer(
        &self,
        mint: &str,
        wallet: &str,
        slot: u64,
        amount_sol: u64,
        amount_tokens: u64,
        run_id: &str,
        component: &str,
    ) -> Option<MarketEvent> {
        let creation_slot = {
            let slots = self.pool_creation_slots.read();
            slots.get(mint).copied()
        }?;

        let slots_after = slot.saturating_sub(creation_slot);

        // Check if within early buyer window
        if slots_after > self.config.early_buyer_slots {
            return None;
        }

        // Check if we already have this wallet as early buyer
        let mut early = self.early_buyers.write();
        let buyers = early.get_mut(mint)?;

        if buyers.iter().any(|b| b.wallet == wallet) {
            return None; // Already recorded
        }

        if buyers.len() >= self.config.max_early_buyers_per_token {
            return None; // Max early buyers reached
        }

        let rank = (buyers.len() + 1) as u32;

        buyers.push(EarlyBuyerInfo {
            wallet: wallet.to_string(),
            slot,
            rank,
            amount_sol,
            amount_tokens,
        });

        info!(
            mint = %mint,
            wallet = %wallet,
            rank = rank,
            slots_after = slots_after,
            amount_sol = amount_sol,
            "Early buyer detected"
        );

        Some(MarketEvent {
            header: RecordHeader::new(component, env!("CARGO_PKG_VERSION"), run_id),
            event_id: format!("early-{}-{}", &mint[..8], rank),
            source: "wallet-tracker".to_string(),
            slot: Some(slot),
            kind: MarketEventKind::EarlyBuyerDetected {
                mint: mint.to_string(),
                buyer_wallet: wallet.to_string(),
                buy_slot: slot,
                slots_after_creation: slots_after,
                amount_sol,
                amount_tokens,
                buyer_rank: rank,
            },
        })
    }

    /// Check if this activity should trigger an insider alert
    fn check_insider_alert(
        &self,
        wallet_type: &WalletType,
        action: &WalletAction,
        mint: &str,
        wallet: &str,
        amount_sol: u64,
        run_id: &str,
        component: &str,
    ) -> Option<MarketEvent> {
        let (alert_type, severity, description) = match (wallet_type, action) {
            (WalletType::DevWallet, WalletAction::Sell) => (
                InsiderAlertType::DevSelling,
                AlertSeverity::Critical,
                format!(
                    "Dev wallet {} selling {} lamports of {}",
                    wallet, amount_sol, mint
                ),
            ),
            (WalletType::KnownBadActor, WalletAction::Buy) => (
                InsiderAlertType::BadActorActive,
                AlertSeverity::Warning,
                format!("Known bad actor {} buying {}", wallet, mint),
            ),
            (WalletType::Whale, WalletAction::Sell)
                if amount_sol >= self.config.whale_threshold_lamports * 2 =>
            {
                (
                    InsiderAlertType::WhaleDumping,
                    AlertSeverity::Warning,
                    format!(
                        "Whale {} dumping {} SOL of {}",
                        wallet,
                        amount_sol / 1_000_000_000,
                        mint
                    ),
                )
            }
            _ => return None,
        };

        warn!(
            alert_type = ?alert_type,
            severity = ?severity,
            mint = %mint,
            wallet = %wallet,
            "Insider alert triggered"
        );

        Some(MarketEvent {
            header: RecordHeader::new(component, env!("CARGO_PKG_VERSION"), run_id),
            event_id: format!("alert-{}-{}", &mint[..8], &wallet[..8]),
            source: "wallet-tracker".to_string(),
            slot: None,
            kind: MarketEventKind::InsiderAlert {
                mint: mint.to_string(),
                alert_type,
                wallets_involved: vec![wallet.to_string()],
                description,
                severity,
            },
        })
    }

    /// Update win rate for a wallet (called after trade outcome is known)
    pub fn update_wallet_win_rate(&self, wallet: &str, win_rate: f64) {
        let mut rates = self.wallet_win_rates.write();
        rates.insert(wallet.to_string(), win_rate);
    }

    /// Get statistics about tracked wallets
    pub fn stats(&self) -> WalletTrackerStats {
        let early = self.early_buyers.read();
        let devs = self.dev_wallets.read();
        let pools = self.pool_creation_slots.read();

        WalletTrackerStats {
            smart_money_count: self.smart_money.len(),
            bad_actor_count: self.bad_actors.len(),
            tracked_tokens: pools.len(),
            dev_wallets_known: devs.len(),
            total_early_buyers: early.values().map(|v| v.len()).sum(),
        }
    }
}

/// Statistics about wallet tracker state
#[derive(Debug, Clone)]
pub struct WalletTrackerStats {
    pub smart_money_count: usize,
    pub bad_actor_count: usize,
    pub tracked_tokens: usize,
    pub dev_wallets_known: usize,
    pub total_early_buyers: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_early_buyer_detection() {
        let config = WalletTrackerCfg {
            enabled: true,
            smart_money_wallets: vec![],
            bad_actor_wallets: vec![],
            early_buyer_slots: 100,
            max_early_buyers_per_token: 10,
            whale_threshold_lamports: 10_000_000_000,
            max_cached_wallets: 1000,
        };

        let tracker = WalletTracker::new(config);

        // Record pool creation at slot 1000
        tracker.record_pool_created("TokenMint123", 1000);

        // First buyer at slot 1010 (10 slots after)
        let events = tracker.process_trade(
            "TokenMint123",
            "Wallet111",
            true,
            1_000_000_000, // 1 SOL
            1_000_000,
            1010,
            "sig123456789012345678901234567890",
            "test-run",
            "test",
        );

        assert_eq!(events.len(), 1);
        if let MarketEventKind::EarlyBuyerDetected { buyer_rank, .. } = &events[0].kind {
            assert_eq!(*buyer_rank, 1);
        } else {
            panic!("Expected EarlyBuyerDetected event");
        }
    }

    #[test]
    fn test_smart_money_detection() {
        let config = WalletTrackerCfg {
            enabled: true,
            smart_money_wallets: vec!["SmartWallet123".to_string()],
            bad_actor_wallets: vec![],
            early_buyer_slots: 100,
            max_early_buyers_per_token: 10,
            whale_threshold_lamports: 10_000_000_000,
            max_cached_wallets: 1000,
        };

        let tracker = WalletTracker::new(config);

        let events = tracker.process_trade(
            "TokenMint123",
            "SmartWallet123",
            true,
            5_000_000_000, // 5 SOL
            1_000_000,
            2000,
            "sig123456789012345678901234567890",
            "test-run",
            "test",
        );

        assert_eq!(events.len(), 1);
        if let MarketEventKind::WalletActivity { wallet_type, .. } = &events[0].kind {
            assert_eq!(*wallet_type, WalletType::SmartMoney);
        } else {
            panic!("Expected WalletActivity event");
        }
    }
}
