//! Capital and Resource Locks for Execution Engine
//!
//! Per DoD §D: Global Arbitration, Locks & No Self-Competition
//! - Capital Locks: reserve SOL + tokens, no overbooking
//! - Resource Locks: pools/accounts that could conflict
//! - Idempotency: prevent duplicate processing
//! - Preemption: higher-priority intents can preempt lower-priority locks (DoD L)

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Unique identifier for a lock holder
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct LockHolder {
    pub intent_id: String,
    pub decision_id: Option<String>,
    /// Priority tier (lower = higher priority). Default: 255 (lowest)
    pub tier: u8,
}

impl LockHolder {
    pub fn new(intent_id: &str) -> Self {
        Self {
            intent_id: intent_id.to_string(),
            decision_id: None,
            tier: 255, // Default lowest priority
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
    AcquiredByPreemption { preempted: LockHolder },
    Conflict { holder: LockHolder },
    InsufficientCapital { available: u64, requested: u64 },
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
        }
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
                    // Preempt the lower-priority lock
                    let preempted = existing.holder.clone();
                    info!(
                        preempting = %holder.intent_id,
                        preempting_tier = holder.tier,
                        preempted = %preempted.intent_id,
                        preempted_tier = preempted.tier,
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
}
