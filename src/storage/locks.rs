//! Capital and Resource Locks for Execution Engine
//!
//! Per DoD §D: Global Arbitration, Locks & No Self-Competition
//! - Capital Locks: reserve SOL + tokens, no overbooking
//! - Resource Locks: pools/accounts that could conflict
//! - Idempotency: prevent duplicate processing
//! - Preemption: higher-priority intents can preempt lower-priority locks (DoD L)
//! - Fairness: prevent starvation via preemption limits (P1)

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Unique identifier for a lock holder
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct LockHolder {
    pub intent_id: String,
    pub decision_id: Option<String>,
    /// Priority tier (lower = higher priority). Default: 255 (lowest)
    pub tier: u8,
    /// Source/strategy name for fairness tracking
    pub source: Option<String>,
}

impl LockHolder {
    pub fn new(intent_id: &str) -> Self {
        Self {
            intent_id: intent_id.to_string(),
            decision_id: None,
            tier: 255, // Default lowest priority
            source: None,
        }
    }

    pub fn with_decision(mut self, decision_id: &str) -> Self {
        self.decision_id = Some(decision_id.to_string());
        self
    }

    /// Set priority tier (lower = higher priority, 0 = highest)
    pub fn with_tier(mut self, tier: u8) -> Self {
        self.tier = tier;
        self
    }

    /// Set source/strategy name for fairness tracking
    pub fn with_source(mut self, source: &str) -> Self {
        self.source = Some(source.to_string());
        self
    }
}

/// Lifecycle phase for a capital lock / reservation (I-20).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapitalLockPhase {
    /// Planning, simulation, cold-path recovery — subject to TTL / `cleanup_expired`.
    PreSend,
    /// TX sent, confirm pending — immune to TTL cleanup; released only at terminal outcome.
    InFlight,
}

/// Capital lock for SOL/token amounts
#[derive(Debug, Clone)]
pub struct CapitalLock {
    pub holder: LockHolder,
    /// Native SOL and/or "quote spend" amount locked, in **lamports**.
    ///
    /// - When [`Self::reserves_trading_wsol`] is `true` (BUY with WSOL
    ///   tracking initialized), this is reserved from `available_wsol` / WSOL ATA
    ///   (same lamport unit as the WSOL token balance).
    /// - When `false`, this is reserved from native SOL (`available_sol`, gas
    ///   and startup path before the first WSOL update).
    pub sol_lamports: u64,
    /// If true, `sol_lamports` were taken from the **WSOL trading** bucket, not
    /// from native `available_sol`. SELLs never set this; they use `tokens` only.
    pub reserves_trading_wsol: bool,
    /// Token amounts locked (mint -> raw amount)
    pub tokens: HashMap<String, u64>,
    /// At lock acquisition: per mint, `available_tokens[mint]` **before** this lock subtracted
    /// `amount` (i.e. the engine's notion of total position for that mint at SELL reservation).
    /// Used for Scope 48 decisions so a concurrent Geyser `set_available_token_balance` cannot
    /// shrink `avail+locked` mid-flight.
    pub token_position_total_at_lock: HashMap<String, u64>,
    pub created_at: Instant,
    pub ttl: Duration,
    pub phase: CapitalLockPhase,
}

/// Resource lock for pools/accounts
#[derive(Debug, Clone)]
pub struct ResourceLock {
    pub holder: LockHolder,
    pub resource_id: String,
    pub resource_type: ResourceType,
    pub created_at: Instant,
    pub ttl: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    Pool,
    Account,
    Mint,
}

/// Lock acquisition result
#[derive(Debug)]
pub enum LockResult {
    Acquired,
    /// Lock acquired by preempting a lower-priority holder
    AcquiredByPreemption {
        preempted: LockHolder,
    },
    Conflict {
        holder: LockHolder,
    },
    InsufficientCapital {
        available: u64,
        requested: u64,
    },
}

// ============================================================================
// P1: Fairness Tracker - prevents starvation from excessive preemption
// ============================================================================

/// Record of a preemption event
#[derive(Debug, Clone)]
pub struct PreemptionEvent {
    /// Source that was preempted
    pub preempted_source: String,
    /// Source that did the preempting
    pub preempting_source: String,
    /// When the preemption occurred
    pub timestamp: Instant,
}

/// Tracks preemption history for fairness policy enforcement
pub struct FairnessTracker {
    /// Preemption events per source (source -> events within window)
    preemption_counts: RwLock<HashMap<String, VecDeque<Instant>>>,

    /// Sources currently in starvation protection (source -> protection_until)
    starved_sources: RwLock<HashMap<String, Instant>>,

    /// Max preemptions before starvation protection activates
    max_preemptions: u32,

    /// Time window for counting preemptions
    window_duration: Duration,

    /// Duration of starvation protection
    protection_duration: Duration,

    /// Whether fairness tracking is enabled
    enabled: bool,
}

impl FairnessTracker {
    pub fn new(
        max_preemptions: u32,
        window_secs: u64,
        protection_secs: u64,
        enabled: bool,
    ) -> Self {
        Self {
            preemption_counts: RwLock::new(HashMap::new()),
            starved_sources: RwLock::new(HashMap::new()),
            max_preemptions,
            window_duration: Duration::from_secs(window_secs),
            protection_duration: Duration::from_secs(protection_secs),
            enabled,
        }
    }

    /// Record a preemption event
    /// Returns true if the preempted source has now reached starvation threshold
    pub fn record_preemption(&self, preempted_source: &str, preempting_source: &str) -> bool {
        if !self.enabled {
            return false;
        }

        let now = Instant::now();
        let cutoff = now - self.window_duration;

        let mut counts = self.preemption_counts.write();
        let events = counts.entry(preempted_source.to_string()).or_default();

        // Remove old events outside the window
        while events.front().map(|t| *t < cutoff).unwrap_or(false) {
            events.pop_front();
        }

        // Add new event
        events.push_back(now);

        let count = events.len() as u32;

        info!(
            preempted = %preempted_source,
            preempting = %preempting_source,
            count_in_window = count,
            max = self.max_preemptions,
            "Preemption recorded"
        );

        // Check if starvation threshold reached
        if count >= self.max_preemptions {
            // Activate starvation protection
            let protection_until = now + self.protection_duration;
            self.starved_sources
                .write()
                .insert(preempted_source.to_string(), protection_until);

            warn!(
                source = %preempted_source,
                preemption_count = count,
                protection_secs = self.protection_duration.as_secs(),
                "Starvation protection activated for source"
            );

            return true;
        }

        false
    }

    /// Check if a source is currently under starvation protection
    /// Returns true if the source should get elevated priority
    pub fn is_starved(&self, source: &str) -> bool {
        if !self.enabled {
            return false;
        }

        let now = Instant::now();
        let starved = self.starved_sources.read();

        if let Some(protection_until) = starved.get(source) {
            if *protection_until > now {
                return true;
            }
        }

        false
    }

    /// Check if preemption should be blocked due to fairness policy
    /// Returns true if the preemption should NOT proceed
    pub fn should_block_preemption(&self, preempted_source: &str, preempting_source: &str) -> bool {
        if !self.enabled {
            return false;
        }

        // If the source being preempted is under starvation protection, block the preemption
        if self.is_starved(preempted_source) {
            info!(
                protected_source = %preempted_source,
                would_preempt = %preempting_source,
                "Preemption blocked: source under starvation protection"
            );
            return true;
        }

        false
    }

    /// Get preemption count for a source within the current window
    pub fn get_preemption_count(&self, source: &str) -> u32 {
        if !self.enabled {
            return 0;
        }

        let now = Instant::now();
        let cutoff = now - self.window_duration;

        let counts = self.preemption_counts.read();
        counts.get(source).map_or(0, |events| {
            events.iter().filter(|t| **t >= cutoff).count() as u32
        })
    }

    /// Clean up expired protection periods
    pub fn cleanup_expired(&self) {
        let now = Instant::now();
        let cutoff = now - self.window_duration;

        // Cleanup expired protection
        self.starved_sources.write().retain(|_, until| *until > now);

        // Cleanup old preemption counts
        let mut counts = self.preemption_counts.write();
        for events in counts.values_mut() {
            while events.front().map(|t| *t < cutoff).unwrap_or(false) {
                events.pop_front();
            }
        }
        counts.retain(|_, events| !events.is_empty());
    }

    /// Get summary stats for monitoring
    pub fn stats(&self) -> FairnessStats {
        let counts = self.preemption_counts.read();
        let starved = self.starved_sources.read();

        FairnessStats {
            sources_tracked: counts.len(),
            sources_protected: starved.len(),
            total_preemptions_in_window: counts.values().map(|v| v.len()).sum(),
        }
    }
}

/// Statistics for fairness monitoring
#[derive(Debug, Clone)]
pub struct FairnessStats {
    pub sources_tracked: usize,
    pub sources_protected: usize,
    pub total_preemptions_in_window: usize,
}

/// Global lock manager for the execution engine
///
/// # Lock order (avoids lock inversion / deadlock)
/// Any path that must hold **more than one** of the balance maps or `capital_locks` at
/// the same time uses this **strict total order** (acquire; release in reverse / drop):
///
/// 1. `capital_locks`
/// 2. `available_sol`
/// 3. `available_wsol`
/// 4. `available_tokens`
///
/// `update_wsol_only`, `update_native_sol_only`, `update_wallet_balances`, and
/// the SOL arm of `update_balances` take `capital_locks` read through the
/// snapshot subtract and corresponding `available_*` write so locked sums and
/// balances do not desynchronize vs `try_lock_capital` / `release_locks`.
/// `set_available_token_balance` holds `capital_locks` read through the per-mint
/// locked sum and the `available_tokens` insert (1 then 4).
///
/// `locked_native_sol_lamports` / `locked_wsol_trading_lamports` use only
/// `capital_locks` **read** — never call them while already holding
/// an `available_*` **write** lock (a previous bug: RHS of `*available_sol = …`
/// with `saturating_sub(self.locked_*())`).
pub struct LockManager {
    /// Available native SOL capital (not locked) - used for gas fees
    available_sol: RwLock<u64>,
    /// Available WSOL capital (not locked) - used for trades (BUY side)
    /// This is what the dashboard should display as "Available WSOL"
    available_wsol: RwLock<u64>,
    /// Whether WSOL balance has been seen at least once (from WalletBalanceUpdate)
    /// Used to distinguish "WSOL=0" from "WSOL not yet initialized"
    wsol_initialized: std::sync::atomic::AtomicBool,
    available_tokens: RwLock<HashMap<String, u64>>,

    /// Active capital locks
    capital_locks: RwLock<HashMap<String, CapitalLock>>, // intent_id -> lock

    /// Active resource locks
    resource_locks: RwLock<HashMap<String, ResourceLock>>, // resource_id -> lock

    /// Processed intent IDs (idempotency)
    processed_intents: RwLock<HashSet<String>>,

    /// Default TTL for locks
    default_ttl: Duration,

    /// P1: Fairness tracker for preemption limits
    fairness: FairnessTracker,
}

impl LockManager {
    pub fn new(initial_sol: u64) -> Self {
        Self {
            available_sol: RwLock::new(initial_sol),
            available_wsol: RwLock::new(0), // Will be updated by WalletBalanceUpdate events
            wsol_initialized: std::sync::atomic::AtomicBool::new(false),
            available_tokens: RwLock::new(HashMap::new()),
            capital_locks: RwLock::new(HashMap::new()),
            resource_locks: RwLock::new(HashMap::new()),
            processed_intents: RwLock::new(HashSet::new()),
            default_ttl: Duration::from_secs(30),
            // P1: Default fairness policy (5 preemptions per 60s, 30s protection)
            fairness: FairnessTracker::new(5, 60, 30, true),
        }
    }

    /// Create with custom fairness policy
    pub fn with_fairness(
        mut self,
        max_preemptions: u32,
        window_secs: u64,
        protection_secs: u64,
        enabled: bool,
    ) -> Self {
        self.fairness =
            FairnessTracker::new(max_preemptions, window_secs, protection_secs, enabled);
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Update available balances (call after balance refresh)
    pub fn update_balances(&self, sol_lamports: u64, tokens: HashMap<String, u64>) {
        {
            // SOL: hold one `capital_locks` read across subtract + `available_sol` write.
            // Token map is a replacement snapshot: keep that write separate (no broad
            // scope over `available_tokens`).
            let cl = self.capital_locks.read();
            let sub_native = Self::sum_locked_native_sol(&cl);
            *self.available_sol.write() = sol_lamports.saturating_sub(sub_native);
        }
        *self.available_tokens.write() = tokens;
    }

    /// Update a single mint's available token balance.
    ///
    /// This is intentionally narrow: execution-engine may learn token balances
    /// opportunistically (e.g., from a SELL preflight check) without having a
    /// complete wallet token snapshot.
    pub fn set_available_token_balance(&self, mint: String, amount_raw: u64) {
        // IMPORTANT:
        // `available_tokens` tracks *unlocked* token balances.
        // A SELL preflight may temporarily observe 0 (ATA not created yet / RPC lag),
        // and we must be able to refresh this value later, otherwise SELL intents can
        // pass the `sell_token_balance` check but still fail at the capital-lock stage.
        //
        // Also, do not overwrite reservations: subtract any active capital locks for
        // this mint so that available_tokens remains "free-to-lock". Hold
        // `capital_locks` read until `insert` so concurrent `try_lock_capital` /
        // `release_locks` cannot desynchronize the per-mint sum and the write
        // (TOCTOU vs SOL/WSOL update paths).
        let cl = self.capital_locks.read();
        let locked_for_mint: u64 = cl
            .values()
            .map(|l| l.tokens.get(&mint).copied().unwrap_or(0))
            .sum();

        let effective_available = amount_raw.saturating_sub(locked_for_mint);

        self.available_tokens
            .write()
            .insert(mint, effective_available);
    }

    /// Sum of native-SOL (non-WSOL) lamports in `capital_locks` (BUY path uses map snapshot).
    fn sum_locked_native_sol(locks: &HashMap<String, CapitalLock>) -> u64 {
        locks
            .values()
            .filter(|l| !l.reserves_trading_wsol)
            .map(|l| l.sol_lamports)
            .sum()
    }

    /// Sum of WSOL-trading lamports in `capital_locks` (BUY side; map snapshot).
    fn sum_locked_wsol_trading(locks: &HashMap<String, CapitalLock>) -> u64 {
        locks
            .values()
            .filter(|l| l.reserves_trading_wsol)
            .map(|l| l.sol_lamports)
            .sum()
    }

    /// Sum of native-SOL (non-WSOL) lamports currently held in capital locks.
    fn locked_native_sol_lamports(&self) -> u64 {
        let locks = self.capital_locks.read();
        Self::sum_locked_native_sol(&locks)
    }

    /// Sum of WSOL-trading lamports currently held in capital locks (BUY side).
    fn locked_wsol_trading_lamports(&self) -> u64 {
        let locks = self.capital_locks.read();
        Self::sum_locked_wsol_trading(&locks)
    }

    /// Add to an existing mint's available token balance (accumulate).
    ///
    /// Used after confirmed BUY fills where each fill is incremental.
    /// Unlike `set_available_token_balance` (which replaces), this ADDS
    /// `additional_raw` to whatever is already tracked for this mint.
    /// This is critical for multi-BUY scenarios (probe + scale-in) where
    /// each BUY delivers additional tokens on top of the existing balance.
    pub fn add_available_token_balance(&self, mint: String, additional_raw: u64) {
        let current = self
            .available_tokens
            .read()
            .get(&mint)
            .copied()
            .unwrap_or(0);
        let new_total = current.saturating_add(additional_raw);
        self.available_tokens.write().insert(mint, new_total);
    }

    /// `(available_unlocked, locked_in_capital_lock)` for `mint` on `intent_id`'s capital lock.
    ///
    /// For a normal in-flight SELL, `available + locked` is the wallet position size the engine
    /// believed before lock release.
    pub fn available_and_locked_tokens_for_intent(
        &self,
        intent_id: &str,
        mint: &str,
    ) -> (u64, u64) {
        let locked = self
            .capital_locks
            .read()
            .get(intent_id)
            .and_then(|l| l.tokens.get(mint).copied())
            .unwrap_or(0);
        let available = self.available_token_balance(mint);
        (available, locked)
    }

    /// Engine position size for `mint` at SELL lock acquisition (`available + locked` before lock),
    /// if `intent_id` holds a capital lock that includes `mint`.
    ///
    /// Stable vs concurrent Geyser updates that call [`Self::set_available_token_balance`] while
    /// the lock is held (see Scope 48).
    pub fn intent_token_position_total_at_lock(&self, intent_id: &str, mint: &str) -> Option<u64> {
        self.capital_locks
            .read()
            .get(intent_id)
            .and_then(|l| l.token_position_total_at_lock.get(mint).copied())
    }

    /// Update wallet balances from WalletBalanceUpdate event (SOL + WSOL).
    ///
    /// This is called when market-data publishes balance updates via NATS.
    /// - `sol_lamports`: Native SOL balance (for gas fees)
    /// - `wsol_lamports`: WSOL ATA balance (for trades)
    ///
    /// The dashboard "Available WSOL" metric should show WSOL, as that's what
    /// is actually used for BUY trades (no in-TX wrapping).
    pub fn update_wallet_balances(&self, sol_lamports: u64, wsol_lamports: Option<u64>) {
        {
            // Single snapshot: native + (optional) WSOL locked sums match one `available_sol` /
            // `available_wsol` pair vs concurrent lock/release.
            let cl = self.capital_locks.read();
            let sub_native = Self::sum_locked_native_sol(&cl);
            *self.available_sol.write() = sol_lamports.saturating_sub(sub_native);
            if let Some(wsol) = wsol_lamports {
                let sub_wsol = Self::sum_locked_wsol_trading(&cl);
                *self.available_wsol.write() = wsol.saturating_sub(sub_wsol);
                // Mark WSOL as initialized once we've seen it
                self.wsol_initialized
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
        tracing::info!(
            sol = sol_lamports,
            wsol = ?wsol_lamports,
            wsol_initialized = self.wsol_initialized.load(std::sync::atomic::Ordering::Relaxed),
            "LockManager wallet balances updated"
        );
    }

    /// Update only native SOL balance (from Geyser NATIVE_SOL event).
    /// Does NOT touch WSOL — each event handler only updates its own value.
    /// Subtracts active capital locks so that total_native_sol() = on-chain value
    /// (avoids double-counting: on-chain already includes locked amounts).
    pub fn update_native_sol_only(&self, sol_lamports: u64) {
        let cl = self.capital_locks.read();
        let sub = Self::sum_locked_native_sol(&cl);
        *self.available_sol.write() = sol_lamports.saturating_sub(sub);
    }

    /// Update only WSOL balance (from Geyser WSOL event).
    /// Does NOT touch native SOL — each event handler only updates its own value.
    pub fn update_wsol_only(&self, wsol_lamports: u64) {
        // Hold `capital_locks` read across the subtract+write to `available_wsol` so
        // the locked sum and `available_wsol` cannot desynchronize vs concurrent
        // `try_lock_capital` / `release_locks` (same order as other paths: 1 then 3).
        let cl = self.capital_locks.read();
        let sub = Self::sum_locked_wsol_trading(&cl);
        *self.available_wsol.write() = wsol_lamports.saturating_sub(sub);
        self.wsol_initialized
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get current available WSOL (for trades/dashboard)
    pub fn available_wsol(&self) -> u64 {
        *self.available_wsol.read()
    }

    /// True after at least one on-chain WSOL balance was applied (Geyser / wallet snapshot).
    /// When true, [`Self::try_lock_capital`] with BUY-sized `sol_lamports` reserves from
    /// [`Self::available_wsol`] (trading capital), not from native `available_sol`.
    pub fn is_wsol_trading_capital_tracked(&self) -> bool {
        self.wsol_initialized
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// `reserves_trading_wsol` for the capital lock held by `intent_id`, if present.
    ///
    /// Use this (not [`Self::is_wsol_trading_capital_tracked`]) to describe a lock that was
    /// just acquired: `wsol_initialized` can change between the decision in
    /// [`Self::try_lock_capital`] and a second read of the atomic.
    pub fn capital_lock_reserves_trading_wsol(&self, intent_id: &str) -> Option<bool> {
        self.capital_locks
            .read()
            .get(intent_id)
            .map(|l| l.reserves_trading_wsol)
    }

    /// Get current available (unlocked) token balance for a mint (raw units).
    pub fn available_token_balance(&self, mint: &str) -> u64 {
        self.available_tokens.read().get(mint).copied().unwrap_or(0)
    }

    /// True when execution-engine has applied at least one `WalletBalanceSnapshot` for this mint.
    ///
    /// Balance may be zero — a snapshot still proves the wallet ATA was observed on-chain (Geyser).
    /// Used to omit idempotent ATA-create instructions in size-constrained cross-DEX arb bundles.
    pub fn token_wallet_snapshot_seen(&self, mint: &str) -> bool {
        self.available_tokens.read().contains_key(mint)
    }

    /// Count the number of token mints with non-zero available balance.
    /// Used as Single Source of Truth for open positions count,
    /// replacing the error-prone dual-path AtomicUsize counter.
    pub fn count_non_zero_token_balances(&self) -> usize {
        self.available_tokens
            .read()
            .values()
            .filter(|&&balance| balance > 0)
            .count()
    }

    /// Snapshot of mints with non-zero available balance (for liquidation retry merge).
    pub fn non_zero_available_token_balances(&self) -> Vec<(String, u64)> {
        self.available_tokens
            .read()
            .iter()
            .filter(|(_, balance)| **balance > 0)
            .map(|(mint, balance)| (mint.clone(), *balance))
            .collect()
    }

    /// Get free-to-spend capital for the next BUY (same rule as [`Self::try_lock_capital`]
    /// for empty `tokens`): [`Self::available_wsol`] after the first WSOL update, else
    /// [`Self::available_sol`].
    pub fn available_trading_capital(&self) -> u64 {
        // Only use WSOL if we've received at least one WSOL balance update
        if self
            .wsol_initialized
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            *self.available_wsol.read()
        } else {
            // Fallback to SOL only before first WSOL update (e.g., at startup)
            *self.available_sol.read()
        }
    }

    /// Check if an intent has already been processed (idempotency)
    pub fn is_duplicate(&self, intent_id: &str) -> bool {
        self.processed_intents.read().contains(intent_id)
    }

    /// Mark an intent as processed
    pub fn mark_processed(&self, intent_id: &str) {
        self.processed_intents.write().insert(intent_id.to_string());
    }

    /// Try to acquire a capital lock (pre-send TTL = [`Self::default_ttl`]).
    pub fn try_lock_capital(
        &self,
        holder: LockHolder,
        sol_lamports: u64,
        tokens: HashMap<String, u64>,
    ) -> LockResult {
        self.try_lock_capital_with_ttl(holder, sol_lamports, tokens, None)
    }

    /// Try to acquire a capital lock with an optional pre-send TTL override.
    pub fn try_lock_capital_with_ttl(
        &self,
        holder: LockHolder,
        sol_lamports: u64,
        tokens: HashMap<String, u64>,
        ttl: Option<Duration>,
    ) -> LockResult {
        // Clean expired pre-send locks first (in-flight reservations are never expired here).
        self.cleanup_expired();

        // BUY: reserve quote spend. After WSOL is known from Geyser, reserve from the
        // WSOL (trading) bucket so we do not under-enforce or mis-account vs native
        // SOL. Before first WSOL update, fall back to native `available_sol`.
        let buy_reserves_wsol = tokens.is_empty()
            && sol_lamports > 0
            && self
                .wsol_initialized
                .load(std::sync::atomic::Ordering::Relaxed);

        // Lock order: `capital_locks` → `available_sol` → `available_wsol` → `available_tokens`
        // (see struct doc; matches `release_locks` / `cleanup_expired`).
        let mut locks = self.capital_locks.write();
        let mut available_sol = self.available_sol.write();
        let mut available_wsol = self.available_wsol.write();
        let mut available_tokens = self.available_tokens.write();

        // Check if already locked by this intent
        if locks.contains_key(&holder.intent_id) {
            return LockResult::Conflict {
                holder: holder.clone(),
            };
        }

        if buy_reserves_wsol {
            if *available_wsol < sol_lamports {
                return LockResult::InsufficientCapital {
                    available: *available_wsol,
                    requested: sol_lamports,
                };
            }
        } else if sol_lamports > 0 && *available_sol < sol_lamports {
            return LockResult::InsufficientCapital {
                available: *available_sol,
                requested: sol_lamports,
            };
        }

        // Check token availability
        for (mint, amount) in &tokens {
            let avail = available_tokens.get(mint).copied().unwrap_or(0);
            if avail < *amount {
                return LockResult::InsufficientCapital {
                    available: avail,
                    requested: *amount,
                };
            }
        }

        // Acquire lock
        if buy_reserves_wsol {
            *available_wsol -= sol_lamports;
        } else if sol_lamports > 0 {
            *available_sol -= sol_lamports;
        }
        let mut token_position_total_at_lock = HashMap::with_capacity(tokens.len());
        for (mint, amount) in &tokens {
            if let Some(avail) = available_tokens.get_mut(mint) {
                let position_total_before = *avail;
                *avail -= amount;
                token_position_total_at_lock.insert(mint.clone(), position_total_before);
            }
        }

        let lock = CapitalLock {
            holder: holder.clone(),
            sol_lamports,
            reserves_trading_wsol: buy_reserves_wsol,
            tokens,
            token_position_total_at_lock,
            created_at: Instant::now(),
            ttl: ttl.unwrap_or(self.default_ttl),
            phase: CapitalLockPhase::PreSend,
        };

        debug!(
            intent_id = %holder.intent_id,
            sol_lamports,
            reserves_wsol = buy_reserves_wsol,
            "Capital lock acquired"
        );

        locks.insert(holder.intent_id.clone(), lock);
        LockResult::Acquired
    }

    /// Try to acquire a resource lock (pre-send TTL = [`Self::default_ttl`]).
    pub fn try_lock_resource(
        &self,
        holder: LockHolder,
        resource_id: &str,
        resource_type: ResourceType,
    ) -> LockResult {
        self.try_lock_resource_with_ttl(holder, resource_id, resource_type, None)
    }

    /// Try to acquire a resource lock with preemption support (DoD L) P0)
    ///
    /// If the resource is locked by a lower-priority intent (higher tier number),
    /// the lock will be preempted and acquired by the higher-priority intent.
    ///
    /// P1: Fairness policy may block preemption if the target source has been
    /// preempted too many times recently (starvation protection).
    pub fn try_lock_resource_with_ttl(
        &self,
        holder: LockHolder,
        resource_id: &str,
        resource_type: ResourceType,
        ttl: Option<Duration>,
    ) -> LockResult {
        self.cleanup_expired();

        let mut locks = self.resource_locks.write();

        // Check if resource is already locked
        if let Some(existing) = locks.get(resource_id) {
            if existing.holder.intent_id != holder.intent_id {
                // Preemption check: lower tier number = higher priority
                if holder.tier < existing.holder.tier {
                    // P1: Check fairness policy before preempting
                    let preempted_source = existing.holder.source.as_deref().unwrap_or("unknown");
                    let preempting_source = holder.source.as_deref().unwrap_or("unknown");

                    if self
                        .fairness
                        .should_block_preemption(preempted_source, preempting_source)
                    {
                        // Fairness policy blocks this preemption
                        return LockResult::Conflict {
                            holder: existing.holder.clone(),
                        };
                    }

                    // Record the preemption for fairness tracking
                    let _became_starved = self
                        .fairness
                        .record_preemption(preempted_source, preempting_source);

                    // Preempt the lower-priority lock
                    let preempted = existing.holder.clone();
                    info!(
                        preempting = %holder.intent_id,
                        preempting_tier = holder.tier,
                        preempting_source = preempting_source,
                        preempted = %preempted.intent_id,
                        preempted_tier = preempted.tier,
                        preempted_source = preempted_source,
                        resource_id,
                        "Resource lock preempted (DoD L)"
                    );
                    // Continue to acquire lock below
                } else {
                    return LockResult::Conflict {
                        holder: existing.holder.clone(),
                    };
                }
            }
        }

        let lock = ResourceLock {
            holder: holder.clone(),
            resource_id: resource_id.to_string(),
            resource_type,
            created_at: Instant::now(),
            ttl: ttl.unwrap_or(self.default_ttl),
        };

        debug!(
            intent_id = %holder.intent_id,
            resource_id,
            "Resource lock acquired"
        );

        // Check if we're preempting (resource was previously locked)
        let was_preempted = locks.get(resource_id).map(|l| l.holder.clone());
        locks.insert(resource_id.to_string(), lock);

        match was_preempted {
            Some(preempted) if preempted.intent_id != holder.intent_id => {
                LockResult::AcquiredByPreemption { preempted }
            }
            _ => LockResult::Acquired,
        }
    }

    /// Extend the pre-send TTL for an intent that already holds a capital lock.
    pub fn renew_capital_lock_ttl(&self, intent_id: &str, ttl: Duration) -> bool {
        let mut locks = self.capital_locks.write();
        if let Some(lock) = locks.get_mut(intent_id) {
            if lock.phase == CapitalLockPhase::PreSend {
                lock.ttl = ttl;
                lock.created_at = Instant::now();
                return true;
            }
        }
        false
    }

    /// After successful TX send: keep the capital reservation (I-20) but stop TTL expiry.
    ///
    /// Pool [`ResourceLock`]s are released here — they only serialize TX *build* for the same
    /// pool/accounts; once the TX is sent this intent will not build a second TX on those resources.
    /// Capital stays reserved until terminal outcome (`release_locks` / confirmed-SELL path).
    pub fn promote_capital_lock_to_in_flight(&self, intent_id: &str) -> bool {
        let promoted = {
            let mut locks = self.capital_locks.write();
            if let Some(lock) = locks.get_mut(intent_id) {
                lock.phase = CapitalLockPhase::InFlight;
                true
            } else {
                false
            }
        };
        if promoted {
            self.release_resource_locks_for_intent(intent_id);
            debug!(intent_id, "Capital lock promoted to in-flight reservation");
        }
        promoted
    }

    /// Count capital locks in the in-flight (post-send) phase.
    pub fn in_flight_reservation_count(&self) -> usize {
        self.capital_locks
            .read()
            .values()
            .filter(|l| l.phase == CapitalLockPhase::InFlight)
            .count()
    }

    /// Release all locks for an intent
    pub fn release_locks(&self, intent_id: &str) {
        self.release_capital_lock_restore_tokens(intent_id, true);
        self.release_resource_locks_for_intent(intent_id);
    }

    /// Release all locks after an **on-chain confirmed** SELL: restore SOL/WSOL from the
    /// capital lock as in [`Self::release_locks`], but do **not** return locked *input*
    /// token amounts to [`Self::available_tokens`]. After a successful full SELL those
    /// tokens are no longer held — use [`Self::set_available_token_balance`] to zero the mint;
    /// after a partial SELL do not replace balances while the lock is held (avoid double-count vs
    /// this release path). This path must not resurrect a ghost `open_positions` count (Invariant
    /// A.28, KNOWN_BUG #5).
    pub fn release_locks_after_confirmed_sell(&self, intent_id: &str) {
        self.release_capital_lock_restore_tokens(intent_id, false);
        self.release_resource_locks_for_intent(intent_id);
    }

    /// Release the capital lock for `intent_id`. If `restore_tokens` is true, return locked
    /// token amounts to `available_tokens` (failure / rejection paths). If false, only
    /// release lamport reservations (e.g. confirmed SELL: tokens are already sold).
    fn release_capital_lock_restore_tokens(&self, intent_id: &str, restore_tokens: bool) {
        if let Some(lock) = self.capital_locks.write().remove(intent_id) {
            let mut available_sol = self.available_sol.write();
            let mut available_wsol = self.available_wsol.write();
            let mut available_tokens = self.available_tokens.write();
            if lock.reserves_trading_wsol {
                *available_wsol += lock.sol_lamports;
            } else {
                *available_sol += lock.sol_lamports;
            }
            if restore_tokens {
                for (mint, amount) in lock.tokens {
                    *available_tokens.entry(mint).or_insert(0) += amount;
                }
            }
            debug!(intent_id, restore_tokens, "Capital lock released");
        }
    }

    fn release_resource_locks_for_intent(&self, intent_id: &str) {
        let mut resource_locks = self.resource_locks.write();
        let to_remove: Vec<_> = resource_locks
            .iter()
            .filter(|(_, lock)| lock.holder.intent_id == intent_id)
            .map(|(k, _)| k.clone())
            .collect();

        for key in to_remove {
            resource_locks.remove(&key);
            debug!(intent_id, resource_id = %key, "Resource lock released");
        }
    }

    /// Cleanup expired pre-send locks. In-flight reservations are never released here.
    ///
    /// Returns the number of pre-send capital locks released by TTL expiry.
    pub fn cleanup_expired(&self) -> usize {
        let now = Instant::now();

        // Cleanup capital locks (pre-send only — in-flight reservations are immune).
        let mut capital_locks = self.capital_locks.write();
        let expired: Vec<_> = capital_locks
            .iter()
            .filter(|(_, lock)| {
                lock.phase == CapitalLockPhase::PreSend
                    && now.duration_since(lock.created_at) > lock.ttl
            })
            .map(|(k, _)| k.clone())
            .collect();
        let pre_send_expired_count = expired.len();

        for key in expired {
            if let Some(lock) = capital_locks.remove(&key) {
                let mut available_sol = self.available_sol.write();
                let mut available_wsol = self.available_wsol.write();
                let mut available_tokens = self.available_tokens.write();
                if lock.reserves_trading_wsol {
                    *available_wsol += lock.sol_lamports;
                } else {
                    *available_sol += lock.sol_lamports;
                }
                for (mint, amount) in lock.tokens {
                    *available_tokens.entry(mint).or_insert(0) += amount;
                }
                warn!(intent_id = %key, "Pre-send capital lock expired and released");
            }
        }
        drop(capital_locks);

        // Cleanup resource locks
        let mut resource_locks = self.resource_locks.write();
        let expired: Vec<_> = resource_locks
            .iter()
            .filter(|(_, lock)| now.duration_since(lock.created_at) > lock.ttl)
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired {
            resource_locks.remove(&key);
            warn!(resource_id = %key, "Resource lock expired and released");
        }

        pre_send_expired_count
    }

    /// Get current available SOL
    pub fn available_sol(&self) -> u64 {
        *self.available_sol.read()
    }

    /// Get total native SOL balance (available + locked **native** lamports only).
    ///
    /// **Does not** include lamports reserved from the WSOL trading bucket; those
    /// are on the ATA and are included in [`Self::total_wsol_lamports`].
    pub fn total_native_sol(&self) -> u64 {
        let available = *self.available_sol.read();
        available.saturating_add(self.locked_native_sol_lamports())
    }

    /// On-chain WSOL token balance: available (not locked to in-flight BUYs) +
    /// lamports reserved for in-flight BUY capital locks.
    pub fn total_wsol_lamports(&self) -> u64 {
        let available = *self.available_wsol.read();
        available.saturating_add(self.locked_wsol_trading_lamports())
    }

    /// Get current WSOL balance (from Geyser WalletBalanceUpdate).
    ///
    /// This is the most reliable WSOL source because it's updated by the
    /// same WalletBalanceUpdate event that sets native SOL, ensuring both
    /// values are consistent for wallet total calculations.
    pub fn wsol_balance(&self) -> u64 {
        self.total_wsol_lamports()
    }

    /// Get active lock count
    pub fn active_lock_count(&self) -> (usize, usize) {
        (
            self.capital_locks.read().len(),
            self.resource_locks.read().len(),
        )
    }

    /// Get processed intent count
    pub fn processed_count(&self) -> usize {
        self.processed_intents.read().len()
    }

    // ========================================================================
    // P1: State Persistence support (DoD K)
    // ========================================================================

    /// Get all processed intent IDs for persistence
    pub fn get_processed_intents(&self) -> Vec<String> {
        self.processed_intents.read().iter().cloned().collect()
    }

    /// Restore processed intent IDs from snapshot
    ///
    /// Note: This replaces the current set. Only call during initialization.
    pub fn set_processed_intents(&self, intents: Vec<String>) {
        let mut processed = self.processed_intents.write();
        processed.clear();
        processed.extend(intents);
    }

    // ========================================================================
    // P1: Fairness Policy Support
    // ========================================================================

    /// Check if a source is under starvation protection
    pub fn is_source_starved(&self, source: &str) -> bool {
        self.fairness.is_starved(source)
    }

    /// Get preemption count for a source
    pub fn get_preemption_count(&self, source: &str) -> u32 {
        self.fairness.get_preemption_count(source)
    }

    /// Get fairness statistics
    pub fn fairness_stats(&self) -> FairnessStats {
        self.fairness.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_concurrent_wsol_update_and_lock_try_no_deadlock() {
        // Smokes the stable lock order: geyser-style `update_wsol_only` (background)
        // vs `try_lock_capital`+`release_locks` in parallel. A hang here would be a
        // regression in cross-thread lock ordering.
        let m = Arc::new(LockManager::new(10_000_000_000));
        m.update_wallet_balances(5_000_000_000, Some(1_000_000_000));
        assert!(matches!(
            m.try_lock_capital(
                LockHolder::new("in-flight-buy"),
                400_000_000,
                HashMap::new()
            ),
            LockResult::Acquired
        ));
        let m1 = m.clone();
        let t1 = thread::spawn(move || {
            for _ in 0..200 {
                m1.update_wsol_only(1_000_000_000);
            }
        });
        let m2 = m.clone();
        let t2 = thread::spawn(move || {
            for i in 0..200 {
                let id = format!("p{}", i);
                let _ = m2.try_lock_capital(
                    LockHolder::new(&id),
                    1_000_000, // 0.000001 WSOL, fits in free 600M
                    HashMap::new(),
                );
                m2.release_locks(&id);
            }
        });
        t1.join().expect("t1");
        t2.join().expect("t2");
        m.release_locks("in-flight-buy");
        assert_eq!(m.total_wsol_lamports(), 1_000_000_000);
    }

    #[test]
    fn test_concurrent_native_sol_update_and_lock_try_consistent_total() {
        // Same TOCTOU class as WSOL, but on the native-SOL (pre-WSOL) path.
        let m = Arc::new(LockManager::new(10_000_000_000));
        assert!(!m.is_wsol_trading_capital_tracked());
        assert!(matches!(
            m.try_lock_capital(
                LockHolder::new("in-flight-buy"),
                400_000_000,
                HashMap::new()
            ),
            LockResult::Acquired
        ));
        let m1 = m.clone();
        let t1 = thread::spawn(move || {
            for _ in 0..200 {
                m1.update_native_sol_only(10_000_000_000);
            }
        });
        let m2 = m.clone();
        let t2 = thread::spawn(move || {
            for i in 0..200 {
                let id = format!("p{}", i);
                let _ = m2.try_lock_capital(LockHolder::new(&id), 1_000_000, HashMap::new());
                m2.release_locks(&id);
            }
        });
        t1.join().expect("t1");
        t2.join().expect("t2");
        m.release_locks("in-flight-buy");
        assert_eq!(m.total_native_sol(), 10_000_000_000);
    }

    #[test]
    fn test_concurrent_update_wallet_balances_and_lock_try_consistent_totals() {
        // Full `update_wallet_balances` re-applies both SOL and WSOL from one snapshot.
        let m = Arc::new(LockManager::new(10_000_000_000));
        m.update_wallet_balances(5_000_000_000, Some(1_000_000_000));
        assert!(matches!(
            m.try_lock_capital(
                LockHolder::new("in-flight-buy"),
                400_000_000,
                HashMap::new()
            ),
            LockResult::Acquired
        ));
        let m1 = m.clone();
        let t1 = thread::spawn(move || {
            for _ in 0..200 {
                m1.update_wallet_balances(5_000_000_000, Some(1_000_000_000));
            }
        });
        let m2 = m.clone();
        let t2 = thread::spawn(move || {
            for i in 0..200 {
                let id = format!("p{}", i);
                let _ = m2.try_lock_capital(LockHolder::new(&id), 1_000_000, HashMap::new());
                m2.release_locks(&id);
            }
        });
        t1.join().expect("t1");
        t2.join().expect("t2");
        m.release_locks("in-flight-buy");
        assert_eq!(m.total_wsol_lamports(), 1_000_000_000);
        assert_eq!(m.total_native_sol(), 5_000_000_000);
    }

    #[test]
    fn test_concurrent_update_balances_sol_and_lock_try_consistent_native_total() {
        // `update_balances` SOL arm + token map replacement: native total must stay exact.
        let m = Arc::new(LockManager::new(10_000_000_000));
        m.update_balances(10_000_000_000, HashMap::new());
        assert!(!m.is_wsol_trading_capital_tracked());
        assert!(matches!(
            m.try_lock_capital(
                LockHolder::new("in-flight-buy"),
                400_000_000,
                HashMap::new()
            ),
            LockResult::Acquired
        ));
        let m1 = m.clone();
        let t1 = thread::spawn(move || {
            for _ in 0..200 {
                m1.update_balances(10_000_000_000, HashMap::new());
            }
        });
        let m2 = m.clone();
        let t2 = thread::spawn(move || {
            for i in 0..200 {
                let id = format!("p{}", i);
                let _ = m2.try_lock_capital(LockHolder::new(&id), 1_000_000, HashMap::new());
                m2.release_locks(&id);
            }
        });
        t1.join().expect("t1");
        t2.join().expect("t2");
        m.release_locks("in-flight-buy");
        assert_eq!(m.total_native_sol(), 10_000_000_000);
    }

    #[test]
    fn test_capital_lock_acquire_release() {
        let manager = LockManager::new(1_000_000_000); // 1 SOL

        let holder = LockHolder::new("intent-1");
        let result = manager.try_lock_capital(holder.clone(), 500_000_000, HashMap::new());

        assert!(matches!(result, LockResult::Acquired));
        assert_eq!(manager.available_sol(), 500_000_000);

        manager.release_locks("intent-1");
        assert_eq!(manager.available_sol(), 1_000_000_000);
    }

    #[test]
    fn test_capital_lock_insufficient() {
        let manager = LockManager::new(100_000_000); // 0.1 SOL

        let holder = LockHolder::new("intent-1");
        let result = manager.try_lock_capital(holder, 500_000_000, HashMap::new());

        assert!(matches!(result, LockResult::InsufficientCapital { .. }));
    }

    #[test]
    fn test_resource_lock_conflict() {
        let manager = LockManager::new(1_000_000_000);

        let holder1 = LockHolder::new("intent-1");
        let holder2 = LockHolder::new("intent-2");

        let result1 = manager.try_lock_resource(holder1, "pool-xyz", ResourceType::Pool);
        assert!(matches!(result1, LockResult::Acquired));

        let result2 = manager.try_lock_resource(holder2, "pool-xyz", ResourceType::Pool);
        assert!(matches!(result2, LockResult::Conflict { .. }));
    }

    #[test]
    fn test_idempotency() {
        let manager = LockManager::new(1_000_000_000);

        assert!(!manager.is_duplicate("intent-1"));

        manager.mark_processed("intent-1");

        assert!(manager.is_duplicate("intent-1"));
        assert!(!manager.is_duplicate("intent-2"));
    }

    // ========================================================================
    // P1: Fairness Policy Tests
    // ========================================================================

    #[test]
    fn test_preemption_with_source_tracking() {
        // Disable fairness for basic preemption test
        let manager = LockManager::new(1_000_000_000).with_fairness(5, 60, 30, false);

        // Tier 1 (low priority) locks resource
        let holder1 = LockHolder::new("intent-1")
            .with_tier(1)
            .with_source("strategy-a");
        let result1 = manager.try_lock_resource(holder1, "pool-xyz", ResourceType::Pool);
        assert!(matches!(result1, LockResult::Acquired));

        // Tier 0 (high priority) preempts
        let holder2 = LockHolder::new("intent-2")
            .with_tier(0)
            .with_source("strategy-b");
        let result2 = manager.try_lock_resource(holder2, "pool-xyz", ResourceType::Pool);
        assert!(matches!(result2, LockResult::AcquiredByPreemption { .. }));
    }

    #[test]
    fn test_fairness_starvation_protection() {
        // Enable fairness with low threshold for testing (2 preemptions)
        let manager = LockManager::new(1_000_000_000).with_fairness(2, 60, 30, true);

        // Preempt strategy-a twice to trigger starvation protection
        for i in 0..2 {
            // Strategy-a (low priority) locks
            let holder_a = LockHolder::new(&format!("intent-a-{}", i))
                .with_tier(1)
                .with_source("strategy-a");
            manager.try_lock_resource(holder_a, "pool-1", ResourceType::Pool);

            // Strategy-b (high priority) preempts
            let holder_b = LockHolder::new(&format!("intent-b-{}", i))
                .with_tier(0)
                .with_source("strategy-b");
            let result = manager.try_lock_resource(holder_b, "pool-1", ResourceType::Pool);
            assert!(matches!(result, LockResult::AcquiredByPreemption { .. }));

            manager.release_locks(&format!("intent-b-{}", i));
        }

        // Strategy-a should now be under starvation protection
        assert!(manager.is_source_starved("strategy-a"));

        // Now strategy-a tries to lock again
        let holder_a3 = LockHolder::new("intent-a-3")
            .with_tier(1)
            .with_source("strategy-a");
        manager.try_lock_resource(holder_a3, "pool-2", ResourceType::Pool);

        // Strategy-b tries to preempt but should be blocked by fairness policy
        let holder_b3 = LockHolder::new("intent-b-3")
            .with_tier(0)
            .with_source("strategy-b");
        let result = manager.try_lock_resource(holder_b3, "pool-2", ResourceType::Pool);
        // Should be conflict because preemption was blocked
        assert!(matches!(result, LockResult::Conflict { .. }));
    }

    #[test]
    fn test_fairness_tracker_preemption_count() {
        let tracker = FairnessTracker::new(5, 60, 30, true);

        // Record some preemptions
        for i in 0..3 {
            let became_starved = tracker.record_preemption("victim", "aggressor");
            assert!(
                !became_starved,
                "Should not be starved after {} preemptions",
                i + 1
            );
        }

        assert_eq!(tracker.get_preemption_count("victim"), 3);
        assert!(!tracker.is_starved("victim"));

        // Record 2 more to reach threshold
        tracker.record_preemption("victim", "aggressor");
        let became_starved = tracker.record_preemption("victim", "aggressor");
        assert!(became_starved, "Should be starved after 5 preemptions");
        assert!(tracker.is_starved("victim"));

        // Verify stats
        let stats = tracker.stats();
        assert_eq!(stats.sources_tracked, 1);
        assert_eq!(stats.sources_protected, 1);
        assert_eq!(stats.total_preemptions_in_window, 5);
    }

    #[test]
    fn test_fairness_disabled() {
        let tracker = FairnessTracker::new(5, 60, 30, false);

        // Even after many preemptions, should never be starved when disabled
        for _ in 0..10 {
            tracker.record_preemption("victim", "aggressor");
        }

        assert!(!tracker.is_starved("victim"));
        assert!(!tracker.should_block_preemption("victim", "aggressor"));
        assert_eq!(tracker.get_preemption_count("victim"), 0);
    }

    // ========================================================================
    // Scope 56: amount-scoped capital — parallel TXs, no global mint serializing
    // ========================================================================

    #[test]
    fn test_parallel_buys_same_quote_mint_do_not_exceed_combined_reservation() {
        // Before any WSOL snapshot, BUYs reserve from native `available_sol` (startup fallback).
        let m = LockManager::new(1_000_000_000);
        let h1 = LockHolder::new("buy-a");
        let h2 = LockHolder::new("buy-b");
        assert!(!m.is_wsol_trading_capital_tracked());
        assert!(matches!(
            m.try_lock_capital(h1, 400_000_000, HashMap::new()),
            LockResult::Acquired
        ));
        assert!(matches!(
            m.try_lock_capital(h2, 400_000_000, HashMap::new()),
            LockResult::Acquired
        ));
        assert_eq!(m.available_sol(), 200_000_000);
        m.release_locks("buy-a");
        m.release_locks("buy-b");
        assert_eq!(m.available_sol(), 1_000_000_000);
    }

    #[test]
    fn test_overlapping_buys_reject_when_sum_exceeds_available() {
        let m = LockManager::new(500_000_000);
        let h1 = LockHolder::new("buy-1");
        let h2 = LockHolder::new("buy-2");
        assert!(matches!(
            m.try_lock_capital(h1, 300_000_000, HashMap::new()),
            LockResult::Acquired
        ));
        let r = m.try_lock_capital(h2, 300_000_000, HashMap::new());
        assert!(matches!(r, LockResult::InsufficientCapital { .. }));
        m.release_locks("buy-1");
    }

    #[test]
    fn test_sell_does_not_block_sell_different_mints_parallel() {
        // Different token mints: both can lock their respective balances.
        let m = LockManager::new(0);
        let mut t = HashMap::new();
        t.insert("TokenMintA".to_string(), 1_000_000u64);
        t.insert("TokenMintB".to_string(), 1_000_000u64);
        m.update_balances(0, t);

        let mut a = HashMap::new();
        a.insert("TokenMintA".to_string(), 400_000u64);
        let mut b = HashMap::new();
        b.insert("TokenMintB".to_string(), 400_000u64);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-a"), 0, a),
            LockResult::Acquired
        ));
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-b"), 0, b),
            LockResult::Acquired
        ));
        m.release_locks("sell-a");
        m.release_locks("sell-b");
    }

    #[test]
    fn test_sell_reservations_same_mint_reject_on_insufficient() {
        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([("BaseMintX".to_string(), 1_000_000u64)]));

        let mut t1 = HashMap::new();
        t1.insert("BaseMintX".to_string(), 600_000u64);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("s1"), 0, t1),
            LockResult::Acquired
        ));

        let mut t2 = HashMap::new();
        t2.insert("BaseMintX".to_string(), 500_000u64);
        let r = m.try_lock_capital(LockHolder::new("s2"), 0, t2);
        assert!(matches!(r, LockResult::InsufficientCapital { .. }));
        m.release_locks("s1");
    }

    #[test]
    fn test_set_available_token_balance_subtracts_in_flight_sell_lock() {
        // Same accounting class as `update_wsol_only`: on-chain amount minus active
        // token capital locks for this mint must stay consistent for `try_lock_capital`.
        const M: &str = "BaseMintY";
        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([(M.to_string(), 1_000_000u64)]));

        let mut sell = HashMap::new();
        sell.insert(M.to_string(), 600_000u64);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-in-flight"), 0, sell),
            LockResult::Acquired
        ));
        m.set_available_token_balance(M.to_string(), 1_000_000);
        assert_eq!(m.available_token_balance(M), 400_000u64);
        m.release_locks("sell-in-flight");
        m.set_available_token_balance(M.to_string(), 1_000_000);
        assert_eq!(m.available_token_balance(M), 1_000_000u64);
    }

    #[test]
    fn test_non_zero_available_token_balances_snapshot() {
        let m = LockManager::new(1_000_000_000);
        assert!(m.non_zero_available_token_balances().is_empty());

        m.set_available_token_balance("MintA".to_string(), 100);
        m.set_available_token_balance("MintB".to_string(), 0);
        m.set_available_token_balance("MintC".to_string(), 200);

        let mut snap = m.non_zero_available_token_balances();
        snap.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            snap,
            vec![("MintA".to_string(), 100), ("MintC".to_string(), 200),]
        );
    }

    #[test]
    fn test_concurrent_set_available_token_balance_and_sell_lock_try() {
        const M: &str = "BaseMintZ";
        let m = Arc::new(LockManager::new(0));
        m.update_balances(0, HashMap::from([(M.to_string(), 1_000_000u64)]));

        let mut sell = HashMap::new();
        sell.insert(M.to_string(), 600_000u64);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-hold"), 0, sell),
            LockResult::Acquired
        ));

        let m1 = m.clone();
        let t1 = thread::spawn(move || {
            for _ in 0..200 {
                m1.set_available_token_balance(M.to_string(), 1_000_000);
            }
        });
        let m2 = m.clone();
        let t2 = thread::spawn(move || {
            for i in 0..200 {
                let id = format!("p{}", i);
                let mut tiny = HashMap::new();
                tiny.insert(M.to_string(), 1_000u64);
                let _ = m2.try_lock_capital(LockHolder::new(&id), 0, tiny);
                m2.release_locks(&id);
            }
        });
        t1.join().expect("t1");
        t2.join().expect("t2");

        m.set_available_token_balance(M.to_string(), 1_000_000);
        assert_eq!(
            m.available_token_balance(M),
            400_000,
            "600k SELL still reserved: free must stay 400k after concurrent refresh + tiny locks"
        );
        m.release_locks("sell-hold");
        m.set_available_token_balance(M.to_string(), 1_000_000);
        assert_eq!(m.available_token_balance(M), 1_000_000u64);
    }

    // --- BUY: WSOL-trading path vs native-SOL fallback ---

    #[test]
    fn test_buy_reserves_wsol_lamports_when_wsol_tracked() {
        let m = LockManager::new(10_000_000_000);
        m.update_wallet_balances(5_000_000_000, Some(1_000_000_000));
        assert!(m.is_wsol_trading_capital_tracked());
        let sol_before = m.available_sol();
        let r = m.try_lock_capital(LockHolder::new("buy-1"), 400_000_000, HashMap::new());
        assert!(matches!(r, LockResult::Acquired));
        assert_eq!(m.available_wsol(), 600_000_000);
        assert_eq!(m.available_trading_capital(), 600_000_000);
        assert_eq!(
            m.available_sol(),
            sol_before,
            "BUY should not touch native when WSOL tracked"
        );
        m.release_locks("buy-1");
        assert_eq!(m.available_wsol(), 1_000_000_000);
    }

    #[test]
    fn test_second_buy_rejects_insufficient_wsol_not_native() {
        let m = LockManager::new(10_000_000_000);
        m.update_wallet_balances(1_000_000_000, Some(500_000_000));
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("b1"), 200_000_000, HashMap::new()),
            LockResult::Acquired
        ));
        let r = m.try_lock_capital(LockHolder::new("b2"), 400_000_000, HashMap::new());
        let LockResult::InsufficientCapital {
            available,
            requested,
        } = r
        else {
            panic!("expected InsufficientCapital");
        };
        assert_eq!(available, 300_000_000, "200k locked from 500k");
        assert_eq!(requested, 400_000_000);
        assert_eq!(
            m.available_sol(),
            1_000_000_000,
            "native SOL is not the BUY budget once WSOL is initialized"
        );
    }

    #[test]
    fn test_update_wsol_only_subtracts_in_flight_wsol_buys() {
        let m = LockManager::new(0);
        m.update_wallet_balances(0, Some(1_000_000_000));
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("b"), 400_000_000, HashMap::new()),
            LockResult::Acquired
        ));
        m.update_wsol_only(1_000_000_000);
        assert_eq!(m.total_wsol_lamports(), 1_000_000_000);
        assert_eq!(m.available_wsol(), 600_000_000);
    }

    /// Output-side mints are not in `tokens: HashMap` for SELL — this documents
    /// that WSOL/quote received is not a second global reservation on the sold mint.
    #[test]
    fn test_sell_locks_only_input_mint_not_wsol_out() {
        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([("TOKEN_BASE".to_string(), 2_000_000u64)]));

        let mut sell = HashMap::new();
        sell.insert("TOKEN_BASE".to_string(), 1_000_000u64);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-1"), 0, sell),
            LockResult::Acquired
        ));
        // WSOL would only appear as a separate key if we also reserved output (we do not).
        assert_eq!(
            m.available_token_balance("So11111111111111111111111111111111111111112"),
            0
        );
    }

    // --- Scope 47: confirmed SELL must not resurrect sold token via lock release ---

    #[test]
    fn test_confirmed_sell_release_does_not_restore_sold_token_to_available() {
        const M: &str = "GhostMint";
        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([(M.to_string(), 1_000_000u64)]));

        let mut sell = HashMap::new();
        sell.insert(M.to_string(), 1_000_000u64);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-confirmed"), 0, sell),
            LockResult::Acquired
        ));

        m.set_available_token_balance(M.to_string(), 0);
        assert_eq!(m.available_token_balance(M), 0);
        assert_eq!(m.count_non_zero_token_balances(), 0);

        m.release_locks_after_confirmed_sell("sell-confirmed");

        assert_eq!(
            m.available_token_balance(M),
            0,
            "must not re-add sold token from released capital lock"
        );
        assert_eq!(m.count_non_zero_token_balances(), 0);
    }

    #[test]
    fn test_failed_sell_release_restores_locked_tokens() {
        const M: &str = "FailedSellMint";
        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([(M.to_string(), 1_000_000u64)]));

        let mut sell = HashMap::new();
        sell.insert(M.to_string(), 800_000u64);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-failed"), 0, sell),
            LockResult::Acquired
        ));
        assert_eq!(m.available_token_balance(M), 200_000);

        m.release_locks("sell-failed");

        assert_eq!(m.available_token_balance(M), 1_000_000);
        assert_eq!(m.count_non_zero_token_balances(), 1);
    }

    #[test]
    fn test_confirmed_sell_release_still_restores_wsol_buy_reservation() {
        let m = LockManager::new(10_000_000_000);
        m.update_wallet_balances(5_000_000_000, Some(1_000_000_000));
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("buy-inflight"), 300_000_000, HashMap::new()),
            LockResult::Acquired
        ));
        assert_eq!(m.available_wsol(), 700_000_000);

        m.release_locks_after_confirmed_sell("buy-inflight");

        assert_eq!(m.available_wsol(), 1_000_000_000);
    }

    // --- Scope 48: partial confirmed SELL leaves residual balance; full SELL clears ---

    #[test]
    fn test_partial_confirmed_sell_skip_balance_replace_then_release_keeps_residual() {
        const M: &str = "ProbeScaleMint";
        let probe = 25_313_868_645u64;
        let scale_in = 56_323_355_801u64;
        let total = probe.saturating_add(scale_in);

        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([(M.to_string(), total)]));

        let mut sell = HashMap::new();
        sell.insert(M.to_string(), probe);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-partial"), 0, sell),
            LockResult::Acquired
        ));
        // In-flight SELL: locked probe is gone from `available_tokens`; residual scale_in remains.
        assert_eq!(m.available_token_balance(M), scale_in);
        assert_eq!(m.count_non_zero_token_balances(), 1);

        // Partial confirmed SELL: engine must NOT `set_available_token_balance(..., 0)` here —
        // balance is already correct until lock release.
        m.release_locks_after_confirmed_sell("sell-partial");

        assert_eq!(
            m.available_token_balance(M),
            scale_in,
            "must not restore sold probe from lock"
        );
        assert_eq!(m.count_non_zero_token_balances(), 1);
    }

    #[test]
    fn test_intent_token_position_total_at_lock_stable_across_geyser_mid_flight() {
        const M: &str = "GeyserRaceMint";
        let total = 1_000_000u64;
        let sell_amt = 600_000u64;
        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([(M.to_string(), total)]));

        let mut sell = HashMap::new();
        sell.insert(M.to_string(), sell_amt);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-race"), 0, sell),
            LockResult::Acquired
        ));

        assert_eq!(
            m.intent_token_position_total_at_lock("sell-race", M),
            Some(total)
        );

        // Geyser: wallet already reflects partial sell (T−S) while SELL lock still reserves S.
        m.set_available_token_balance(M.to_string(), total.saturating_sub(sell_amt));
        let (avail, locked) = m.available_and_locked_tokens_for_intent("sell-race", M);
        assert!(
            avail.saturating_add(locked) < total,
            "naive avail+locked must not equal pre-lock position under concurrent Geyser replace"
        );
        assert_eq!(
            m.intent_token_position_total_at_lock("sell-race", M),
            Some(total)
        );
    }

    #[test]
    fn test_full_confirmed_sell_zero_then_release_stays_zero() {
        const M: &str = "FullSellMint";
        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([(M.to_string(), 1_000_000u64)]));

        let mut sell = HashMap::new();
        sell.insert(M.to_string(), 1_000_000u64);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-full"), 0, sell),
            LockResult::Acquired
        ));

        m.set_available_token_balance(M.to_string(), 0);
        m.release_locks_after_confirmed_sell("sell-full");

        assert_eq!(m.available_token_balance(M), 0);
        assert_eq!(m.count_non_zero_token_balances(), 0);
    }

    #[test]
    fn test_available_and_locked_tokens_for_intent() {
        const M: &str = "LockViewMint";
        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([(M.to_string(), 100u64)]));

        let mut sell = HashMap::new();
        sell.insert(M.to_string(), 30u64);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("sell-1"), 0, sell),
            LockResult::Acquired
        ));

        assert_eq!(
            m.available_and_locked_tokens_for_intent("sell-1", M),
            (70, 30)
        );
        assert_eq!(
            m.available_and_locked_tokens_for_intent("missing", M),
            (70, 0)
        );
    }

    #[test]
    fn test_intent_token_position_total_at_lock_matches_avail_plus_locked() {
        const M: &str = "TotalAtLockMint";
        let m = LockManager::new(0);
        m.update_balances(0, HashMap::from([(M.to_string(), 100u64)]));
        let mut sell = HashMap::new();
        sell.insert(M.to_string(), 30u64);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("x"), 0, sell),
            LockResult::Acquired
        ));
        assert_eq!(m.intent_token_position_total_at_lock("x", M), Some(100));
    }

    // --- Capital lock lifecycle: pre-send TTL vs in-flight reservation (I-20) ---

    #[test]
    fn test_in_flight_reservation_survives_cleanup_expired() {
        let m = LockManager::new(1_000_000_000).with_ttl(Duration::from_millis(1));
        assert!(matches!(
            m.try_lock_capital_with_ttl(
                LockHolder::new("sent-wait-confirm"),
                400_000_000,
                HashMap::new(),
                Some(Duration::from_millis(1)),
            ),
            LockResult::Acquired
        ));
        assert_eq!(m.available_sol(), 600_000_000);
        assert!(m.promote_capital_lock_to_in_flight("sent-wait-confirm"));
        assert_eq!(m.in_flight_reservation_count(), 1);

        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(m.cleanup_expired(), 0);
        assert_eq!(m.available_sol(), 600_000_000);
        assert_eq!(m.in_flight_reservation_count(), 1);
    }

    #[test]
    fn test_second_intent_cannot_overbook_while_in_flight_reservation_active() {
        let m = LockManager::new(500_000_000);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("first"), 400_000_000, HashMap::new()),
            LockResult::Acquired
        ));
        assert!(m.promote_capital_lock_to_in_flight("first"));
        let r = m.try_lock_capital(LockHolder::new("second"), 200_000_000, HashMap::new());
        assert!(matches!(r, LockResult::InsufficientCapital { .. }));
        m.release_locks("first");
        assert_eq!(m.available_sol(), 500_000_000);
    }

    #[test]
    fn test_in_flight_reservation_released_on_terminal_outcome() {
        let m = LockManager::new(1_000_000_000);
        assert!(matches!(
            m.try_lock_capital(LockHolder::new("confirmed"), 300_000_000, HashMap::new()),
            LockResult::Acquired
        ));
        assert!(m.promote_capital_lock_to_in_flight("confirmed"));
        assert_eq!(m.in_flight_reservation_count(), 1);
        m.release_locks("confirmed");
        assert_eq!(m.in_flight_reservation_count(), 0);
        assert_eq!(m.available_sol(), 1_000_000_000);
    }

    #[test]
    fn test_pre_send_lock_expired_by_cleanup_expired() {
        let m = LockManager::new(1_000_000_000).with_ttl(Duration::from_millis(1));
        assert!(matches!(
            m.try_lock_capital_with_ttl(
                LockHolder::new("planning"),
                250_000_000,
                HashMap::new(),
                Some(Duration::from_millis(1)),
            ),
            LockResult::Acquired
        ));
        assert_eq!(m.available_sol(), 750_000_000);
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(m.cleanup_expired(), 1);
        assert_eq!(m.available_sol(), 1_000_000_000);
        assert_eq!(m.in_flight_reservation_count(), 0);
    }

    #[test]
    fn test_promote_releases_pool_resource_locks_not_capital() {
        let m = LockManager::new(1_000_000_000);
        let holder = LockHolder::new("pool-tx");
        assert!(matches!(
            m.try_lock_resource(holder.clone(), "pool-abc", ResourceType::Pool),
            LockResult::Acquired
        ));
        assert!(matches!(
            m.try_lock_capital(holder, 100_000_000, HashMap::new()),
            LockResult::Acquired
        ));
        let (_, res_before) = m.active_lock_count();
        assert_eq!(res_before, 1);
        assert!(m.promote_capital_lock_to_in_flight("pool-tx"));
        let (cap, res_after) = m.active_lock_count();
        assert_eq!(cap, 1, "capital reservation must remain");
        assert_eq!(res_after, 0, "pool lock released after send");
        assert_eq!(m.available_sol(), 900_000_000);
        m.release_locks("pool-tx");
    }
}
