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

/// Capital lock for SOL/token amounts
#[derive(Debug, Clone)]
pub struct CapitalLock {
    pub holder: LockHolder,
    /// SOL amount locked (lamports)
    pub sol_lamports: u64,
    /// Token amounts locked (mint -> raw amount)
    pub tokens: HashMap<String, u64>,
    pub created_at: Instant,
    pub ttl: Duration,
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
        let events = counts
            .entry(preempted_source.to_string())
            .or_default();

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
pub struct LockManager {
    /// Available capital (not locked)
    available_sol: RwLock<u64>,
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
        *self.available_sol.write() = sol_lamports;
        *self.available_tokens.write() = tokens;
    }

    /// Update a single mint's available token balance.
    ///
    /// This is intentionally narrow: execution-engine may learn token balances
    /// opportunistically (e.g., from a SELL preflight check) without having a
    /// complete wallet token snapshot.
    pub fn set_available_token_balance(&self, mint: String, amount_raw: u64) {
        self.available_tokens
            .write()
            .entry(mint)
            .or_insert(amount_raw);
    }

    /// Check if an intent has already been processed (idempotency)
    pub fn is_duplicate(&self, intent_id: &str) -> bool {
        self.processed_intents.read().contains(intent_id)
    }

    /// Mark an intent as processed
    pub fn mark_processed(&self, intent_id: &str) {
        self.processed_intents.write().insert(intent_id.to_string());
    }

    /// Try to acquire a capital lock
    pub fn try_lock_capital(
        &self,
        holder: LockHolder,
        sol_lamports: u64,
        tokens: HashMap<String, u64>,
    ) -> LockResult {
        // Clean expired locks first
        self.cleanup_expired();

        let mut available = self.available_sol.write();
        let mut available_tokens = self.available_tokens.write();
        let mut locks = self.capital_locks.write();

        // Check if already locked by this intent
        if locks.contains_key(&holder.intent_id) {
            return LockResult::Conflict {
                holder: holder.clone(),
            };
        }

        // Check SOL availability
        if *available < sol_lamports {
            return LockResult::InsufficientCapital {
                available: *available,
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
        *available -= sol_lamports;
        for (mint, amount) in &tokens {
            if let Some(avail) = available_tokens.get_mut(mint) {
                *avail -= amount;
            }
        }

        let lock = CapitalLock {
            holder: holder.clone(),
            sol_lamports,
            tokens,
            created_at: Instant::now(),
            ttl: self.default_ttl,
        };

        debug!(
            intent_id = %holder.intent_id,
            sol_lamports,
            "Capital lock acquired"
        );

        locks.insert(holder.intent_id.clone(), lock);
        LockResult::Acquired
    }

    /// Try to acquire a resource lock with preemption support (DoD L) P0)
    ///
    /// If the resource is locked by a lower-priority intent (higher tier number),
    /// the lock will be preempted and acquired by the higher-priority intent.
    ///
    /// P1: Fairness policy may block preemption if the target source has been
    /// preempted too many times recently (starvation protection).
    pub fn try_lock_resource(
        &self,
        holder: LockHolder,
        resource_id: &str,
        resource_type: ResourceType,
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
            ttl: self.default_ttl,
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

    /// Release all locks for an intent
    pub fn release_locks(&self, intent_id: &str) {
        // Release capital lock
        if let Some(lock) = self.capital_locks.write().remove(intent_id) {
            *self.available_sol.write() += lock.sol_lamports;
            let mut available_tokens = self.available_tokens.write();
            for (mint, amount) in lock.tokens {
                *available_tokens.entry(mint).or_insert(0) += amount;
            }
            debug!(intent_id, "Capital lock released");
        }

        // Release resource locks
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

    /// Cleanup expired locks
    pub fn cleanup_expired(&self) {
        let now = Instant::now();

        // Cleanup capital locks
        let mut capital_locks = self.capital_locks.write();
        let expired: Vec<_> = capital_locks
            .iter()
            .filter(|(_, lock)| now.duration_since(lock.created_at) > lock.ttl)
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired {
            if let Some(lock) = capital_locks.remove(&key) {
                *self.available_sol.write() += lock.sol_lamports;
                let mut available_tokens = self.available_tokens.write();
                for (mint, amount) in lock.tokens {
                    *available_tokens.entry(mint).or_insert(0) += amount;
                }
                warn!(intent_id = %key, "Capital lock expired and released");
            }
        }

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
    }

    /// Get current available SOL
    pub fn available_sol(&self) -> u64 {
        *self.available_sol.read()
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
}
