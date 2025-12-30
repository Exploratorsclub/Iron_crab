//! Reason Codes for Decision Rejects (DoD §E, §J)
//!
//! Each rejection must have exactly one primary reason_code.
//! These are used in DecisionRecord + Prometheus counter labels.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Canonical reason codes for intent rejection
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RejectReason {
    // === TTL / Deadline ===
    /// Intent TTL expired before processing
    TtlExpired,
    /// Intent deadline slot already passed
    DeadlineSlotPassed,

    // === Risk Limits ===
    /// Daily loss limit would be exceeded
    RiskDailyLossLimit,
    /// Max position size would be exceeded
    RiskMaxPosition,
    /// Max open positions limit reached
    RiskMaxOpenPositions,
    /// Drawdown scaling rejected trade
    RiskDrawdownLimit,
    /// Cooldown active for this mint
    RiskCooldownActive,

    // === Simulation ===
    /// Simulation returned error
    SimFailed,
    /// Simulation showed insufficient output (slippage)
    SimSlippageExceeded,
    /// Simulation showed insufficient balance
    SimInsufficientBalance,
    /// Simulation timed out
    SimTimeout,

    // === Locks / Conflicts ===
    /// Capital lock conflict (funds already reserved)
    LockCapitalConflict,
    /// Resource lock conflict (pool/account in use)
    LockResourceConflict,
    /// Intent already processed (idempotency)
    LockDuplicateIntent,

    // === Data / Validation ===
    /// Missing decimals for mint
    MissingDecimals,
    /// Invalid mint address
    InvalidMint,
    /// Pool not found
    PoolNotFound,
    /// Quote unavailable
    QuoteUnavailable,
    /// Invalid intent fields
    InvalidIntent,

    // === System ===
    /// Kill switch activated
    KillSwitchActive,
    /// Internal error
    InternalError,
    /// Unknown reason (should not happen)
    Unknown,

    // === P1: Bundle / Jito ===
    /// Jito bundle submission failed
    BundleFailed,
    /// Bundle not confirmed within timeout
    BundleTimeout,
    /// Bundle required but Jito not configured
    BundleNotConfigured,
}

impl RejectReason {
    /// Get the canonical string representation for logging/metrics
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TtlExpired => "TTL_EXPIRED",
            Self::DeadlineSlotPassed => "DEADLINE_SLOT_PASSED",
            Self::RiskDailyLossLimit => "RISK_DAILY_LOSS_LIMIT",
            Self::RiskMaxPosition => "RISK_MAX_POSITION",
            Self::RiskMaxOpenPositions => "RISK_MAX_OPEN_POSITIONS",
            Self::RiskDrawdownLimit => "RISK_DRAWDOWN_LIMIT",
            Self::RiskCooldownActive => "RISK_COOLDOWN_ACTIVE",
            Self::SimFailed => "SIM_FAILED",
            Self::SimSlippageExceeded => "SIM_SLIPPAGE_EXCEEDED",
            Self::SimInsufficientBalance => "SIM_INSUFFICIENT_BALANCE",
            Self::SimTimeout => "SIM_TIMEOUT",
            Self::LockCapitalConflict => "LOCK_CAPITAL_CONFLICT",
            Self::LockResourceConflict => "LOCK_RESOURCE_CONFLICT",
            Self::LockDuplicateIntent => "LOCK_DUPLICATE_INTENT",
            Self::MissingDecimals => "MISSING_DECIMALS",
            Self::InvalidMint => "INVALID_MINT",
            Self::PoolNotFound => "POOL_NOT_FOUND",
            Self::QuoteUnavailable => "QUOTE_UNAVAILABLE",
            Self::InvalidIntent => "INVALID_INTENT",
            Self::KillSwitchActive => "KILL_SWITCH_ACTIVE",
            Self::InternalError => "INTERNAL_ERROR",
            Self::Unknown => "UNKNOWN",
            Self::BundleFailed => "BUNDLE_FAILED",
            Self::BundleTimeout => "BUNDLE_TIMEOUT",
            Self::BundleNotConfigured => "BUNDLE_NOT_CONFIGURED",
        }
    }

    /// Parse from string (for deserialization from logs)
    pub fn from_str_loose(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "TTL_EXPIRED" => Self::TtlExpired,
            "DEADLINE_SLOT_PASSED" => Self::DeadlineSlotPassed,
            "RISK_DAILY_LOSS_LIMIT" => Self::RiskDailyLossLimit,
            "RISK_MAX_POSITION" => Self::RiskMaxPosition,
            "RISK_MAX_OPEN_POSITIONS" => Self::RiskMaxOpenPositions,
            "RISK_DRAWDOWN_LIMIT" => Self::RiskDrawdownLimit,
            "RISK_COOLDOWN_ACTIVE" => Self::RiskCooldownActive,
            "SIM_FAILED" => Self::SimFailed,
            "SIM_SLIPPAGE_EXCEEDED" => Self::SimSlippageExceeded,
            "SIM_INSUFFICIENT_BALANCE" => Self::SimInsufficientBalance,
            "SIM_TIMEOUT" => Self::SimTimeout,
            "LOCK_CAPITAL_CONFLICT" => Self::LockCapitalConflict,
            "LOCK_RESOURCE_CONFLICT" => Self::LockResourceConflict,
            "LOCK_DUPLICATE_INTENT" => Self::LockDuplicateIntent,
            "MISSING_DECIMALS" => Self::MissingDecimals,
            "INVALID_MINT" => Self::InvalidMint,
            "POOL_NOT_FOUND" => Self::PoolNotFound,
            "QUOTE_UNAVAILABLE" => Self::QuoteUnavailable,
            "INVALID_INTENT" => Self::InvalidIntent,
            "KILL_SWITCH_ACTIVE" => Self::KillSwitchActive,
            "INTERNAL_ERROR" => Self::InternalError,
            "BUNDLE_FAILED" => Self::BundleFailed,
            "BUNDLE_TIMEOUT" => Self::BundleTimeout,
            "BUNDLE_NOT_CONFIGURED" => Self::BundleNotConfigured,
            _ => Self::Unknown,
        }
    }

    /// Check if this is a risk-related rejection
    pub fn is_risk_related(&self) -> bool {
        matches!(
            self,
            Self::RiskDailyLossLimit
                | Self::RiskMaxPosition
                | Self::RiskMaxOpenPositions
                | Self::RiskDrawdownLimit
                | Self::RiskCooldownActive
        )
    }

    /// Check if this is a simulation-related rejection
    pub fn is_simulation_related(&self) -> bool {
        matches!(
            self,
            Self::SimFailed
                | Self::SimSlippageExceeded
                | Self::SimInsufficientBalance
                | Self::SimTimeout
        )
    }

    /// Check if this is a lock-related rejection
    pub fn is_lock_related(&self) -> bool {
        matches!(
            self,
            Self::LockCapitalConflict | Self::LockResourceConflict | Self::LockDuplicateIntent
        )
    }

    /// P1: Check if this is a bundle/Jito-related rejection
    pub fn is_bundle_related(&self) -> bool {
        matches!(
            self,
            Self::BundleFailed | Self::BundleTimeout | Self::BundleNotConfigured
        )
    }
}

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reason_code_roundtrip() {
        for reason in [
            RejectReason::TtlExpired,
            RejectReason::SimFailed,
            RejectReason::LockCapitalConflict,
            RejectReason::RiskDailyLossLimit,
        ] {
            let s = reason.as_str();
            let parsed = RejectReason::from_str_loose(s);
            assert_eq!(reason, parsed);
        }
    }

    #[test]
    fn test_reason_categories() {
        assert!(RejectReason::RiskMaxPosition.is_risk_related());
        assert!(!RejectReason::RiskMaxPosition.is_simulation_related());

        assert!(RejectReason::SimFailed.is_simulation_related());
        assert!(!RejectReason::SimFailed.is_risk_related());

        assert!(RejectReason::LockCapitalConflict.is_lock_related());
    }

    #[test]
    fn test_serde_reason() {
        let reason = RejectReason::SimSlippageExceeded;
        let json = serde_json::to_string(&reason).unwrap();
        assert_eq!(json, "\"SIM_SLIPPAGE_EXCEEDED\"");

        let parsed: RejectReason = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, reason);
    }
}
