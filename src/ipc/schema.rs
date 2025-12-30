//! Core IPC Schema Types
//!
//! Per docs/STORAGE_CONVENTIONS.md, every record must have:
//! - schema_version (u32)
//! - ts_unix_ms (u64)
//! - component (string)
//! - build (string)
//! - run_id (string/uuid)

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Current schema version for all IPC types
pub const SCHEMA_VERSION: u32 = 1;

// ============================================================================
// Common Header (embedded in each record type)
// ============================================================================

/// Common header fields required by STORAGE_CONVENTIONS.md
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecordHeader {
    pub schema_version: u32,
    pub ts_unix_ms: u64,
    pub component: String,
    pub build: String,
    pub run_id: String,
}

impl RecordHeader {
    pub fn new(component: &str, build: &str, run_id: &str) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            ts_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            component: component.to_string(),
            build: build.to_string(),
            run_id: run_id.to_string(),
        }
    }
}

// ============================================================================
// Amount with explicit units (P0 requirement: no implicit decimals)
// ============================================================================

/// Amount with explicit unit specification
/// P0 requirement: Units/Decimals explizit – keine impliziten UI/raw Konventionen
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExplicitAmount {
    /// Raw on-chain value (lamports for SOL, smallest unit for tokens)
    pub raw: u64,
    /// Number of decimals for this token (e.g., 9 for SOL, 6 for USDC)
    pub decimals: u8,
    /// Optional: UI representation (raw / 10^decimals), for logging convenience only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui: Option<Decimal>,
}

impl ExplicitAmount {
    pub fn new(raw: u64, decimals: u8) -> Self {
        let ui = Decimal::from(raw) / Decimal::from(10u64.pow(decimals as u32));
        Self {
            raw,
            decimals,
            ui: Some(ui),
        }
    }

    pub fn zero(decimals: u8) -> Self {
        Self {
            raw: 0,
            decimals,
            ui: Some(Decimal::ZERO),
        }
    }
}

// ============================================================================
// MarketEvent (produced by market-data)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
pub enum MarketEventKind {
    /// New pool/market discovered
    PoolCreated {
        pool_address: String,
        base_mint: String,
        quote_mint: String,
        dex: String,
        initial_liquidity_sol: Option<Decimal>,
    },
    /// Swap observed on-chain
    SwapObserved {
        pool_address: String,
        signature: String,
        side: String, // "buy" or "sell"
        amount_in: ExplicitAmount,
        amount_out: ExplicitAmount,
    },
    /// Price/quote update
    PriceUpdate {
        pool_address: String,
        base_mint: String,
        quote_mint: String,
        price: Decimal,
        liquidity_sol: Option<Decimal>,
    },
    /// Slot progression (heartbeat)
    SlotUpdate { current_slot: u64 },
    /// Raw account update from Geyser
    AccountUpdate {
        pubkey: String,
        owner: String,
        data_len: usize,
    },
    /// Transaction detected via Geyser
    TransactionDetected {
        signature: String,
        program: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketEvent {
    #[serde(flatten)]
    pub header: RecordHeader,
    pub event_id: String,
    pub source: String, // "geyser", "rpc", "ws"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<u64>,
    #[serde(flatten)]
    pub kind: MarketEventKind,
}

impl MarketEvent {
    pub fn new(
        component: &str,
        build: &str,
        run_id: &str,
        event_id: String,
        source: &str,
        slot: Option<u64>,
        kind: MarketEventKind,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            event_id,
            source: source.to_string(),
            slot,
            kind,
        }
    }
}

// ============================================================================
// TradeIntent (produced by strategy bots)
// ============================================================================

/// Intent tier for arbitration priority
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum IntentTier {
    /// Tier 0: highest priority (e.g., emergency exits, MEV)
    Tier0 = 0,
    /// Tier 1: normal priority (e.g., momentum trades)
    Tier1 = 1,
}

/// Origin type for Typ A vs Typ B classification (DoD D.1)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IntentOrigin {
    /// Typ A: market-driven strategy (e.g., momentum, strategy arbitrage)
    StrategyA,
    /// Typ B: reactive/tx-dependent MEV (e.g., backrun, bundle optimization)
    ExecutionMevB,
}

/// Regime classification for momentum policies
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TradingRegime {
    /// Early phase: thin data, high manipulation risk, strict filters
    Early,
    /// Established phase: more data, classic momentum signals
    Established,
    /// Not applicable (e.g., pure arbitrage)
    NotApplicable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradeIntent {
    #[serde(flatten)]
    pub header: RecordHeader,

    // === Required fields per DoD B) ===
    pub intent_id: String,
    pub source: String, // "momentum-bot", "arb-strategy", "execution-worker"
    pub tier: IntentTier,
    pub origin_type: IntentOrigin,

    /// Deadline: slot number or None if TTL-based
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_slot: Option<u64>,
    /// TTL in milliseconds from creation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,

    /// Capital required (SOL, explicit units)
    pub required_capital: ExplicitAmount,

    /// Resources: mints/pools/accounts involved
    pub resources: TradeResources,

    /// Expected value / ROI in basis points
    pub expected_roi_bps: i32,

    /// Maximum acceptable slippage in basis points
    pub max_slippage_bps: u32,

    /// Trade direction
    pub side: TradeSide,

    /// Regime classification
    pub regime: TradingRegime,

    /// Optional: trigger event that caused this intent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TradeResources {
    pub input_mint: String,
    pub output_mint: String,
    pub pools: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub accounts: Vec<String>,
}

impl TradeIntent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        component: &str,
        build: &str,
        run_id: &str,
        intent_id: String,
        source: &str,
        tier: IntentTier,
        origin_type: IntentOrigin,
        required_capital: ExplicitAmount,
        resources: TradeResources,
        expected_roi_bps: i32,
        max_slippage_bps: u32,
        side: TradeSide,
        regime: TradingRegime,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            intent_id,
            source: source.to_string(),
            tier,
            origin_type,
            deadline_slot: None,
            ttl_ms: Some(5000), // default 5s TTL
            required_capital,
            resources,
            expected_roi_bps,
            max_slippage_bps,
            side,
            regime,
            trigger_event_id: None,
        }
    }

    pub fn with_deadline_slot(mut self, slot: u64) -> Self {
        self.deadline_slot = Some(slot);
        self.ttl_ms = None;
        self
    }

    pub fn with_ttl_ms(mut self, ttl: u64) -> Self {
        self.ttl_ms = Some(ttl);
        self.deadline_slot = None;
        self
    }

    pub fn with_trigger(mut self, event_id: String) -> Self {
        self.trigger_event_id = Some(event_id);
        self
    }
}

// ============================================================================
// DecisionRecord (produced by execution-engine)
// ============================================================================

/// Check result for decision audit trail
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckResult {
    pub check_name: String,
    pub passed: bool,
    pub reason_code: Option<String>,
    pub details: Option<String>,
}

/// Simulation result summary
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SimulationResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_units_consumed: Option<u64>,
}

/// Send result summary
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SendResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bundle_id: Option<String>,
    pub sent_at_ms: u64,
}

/// Final decision outcome
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DecisionOutcome {
    /// Intent rejected before planning
    Rejected,
    /// Intent expired (TTL/deadline)
    Expired,
    /// Simulation failed
    SimFailed,
    /// Transaction sent (awaiting confirmation)
    Sent,
    /// Transaction confirmed on-chain
    Confirmed,
    /// Transaction failed after send
    FailedConfirmed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecisionRecord {
    #[serde(flatten)]
    pub header: RecordHeader,

    pub decision_id: String,
    pub intent_id: String,
    pub origin_type: IntentOrigin,
    pub regime: TradingRegime,

    /// All checks performed with pass/fail and reason codes
    pub checks: Vec<CheckResult>,

    /// Primary rejection reason (if rejected)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_reject_reason: Option<String>,

    /// Hash of the transaction plan (for correlation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,

    /// Simulation result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub simulate: Option<SimulationResult>,

    /// Send result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send: Option<SendResult>,

    /// Final outcome
    pub outcome: DecisionOutcome,

    /// Config snapshot ID (for replay)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_snapshot_id: Option<String>,

    /// Input snapshots for replay
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub input_snapshots: HashMap<String, String>,
}

impl DecisionRecord {
    pub fn new_rejected(
        component: &str,
        build: &str,
        run_id: &str,
        decision_id: String,
        intent_id: String,
        origin_type: IntentOrigin,
        regime: TradingRegime,
        checks: Vec<CheckResult>,
        primary_reject_reason: String,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            decision_id,
            intent_id,
            origin_type,
            regime,
            checks,
            primary_reject_reason: Some(primary_reject_reason),
            plan_hash: None,
            simulate: None,
            send: None,
            outcome: DecisionOutcome::Rejected,
            config_snapshot_id: None,
            input_snapshots: HashMap::new(),
        }
    }

    pub fn new_sim_failed(
        component: &str,
        build: &str,
        run_id: &str,
        decision_id: String,
        intent_id: String,
        origin_type: IntentOrigin,
        regime: TradingRegime,
        checks: Vec<CheckResult>,
        plan_hash: String,
        sim_result: SimulationResult,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            decision_id,
            intent_id,
            origin_type,
            regime,
            checks,
            primary_reject_reason: sim_result.error_code.clone(),
            plan_hash: Some(plan_hash),
            simulate: Some(sim_result),
            send: None,
            outcome: DecisionOutcome::SimFailed,
            config_snapshot_id: None,
            input_snapshots: HashMap::new(),
        }
    }

    pub fn with_config_snapshot(mut self, snapshot_id: String) -> Self {
        self.config_snapshot_id = Some(snapshot_id);
        self
    }

    pub fn with_input_snapshot(mut self, key: String, value: String) -> Self {
        self.input_snapshots.insert(key, value);
        self
    }
}

// ============================================================================
// ExecutionResult (produced by execution-engine after send/confirm)
// ============================================================================

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionStatus {
    Sent,
    Confirmed,
    Failed,
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionFees {
    /// Network fee in lamports
    pub network_fee_lamports: u64,
    /// Priority fee tip in lamports
    pub tip_lamports: u64,
    /// Compute units consumed
    pub compute_units: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionPnl {
    /// Gross PnL (before fees) in lamports
    pub gross_lamports: i64,
    /// Net PnL (after fees) in lamports
    pub net_lamports: i64,
    /// Units are always lamports (9 decimals for SOL)
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionResult {
    #[serde(flatten)]
    pub header: RecordHeader,

    pub execution_id: String,
    pub decision_id: String,
    pub intent_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bundle_id: Option<String>,

    pub status: ExecutionStatus,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_slot: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fees: Option<ExecutionFees>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pnl: Option<ExecutionPnl>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,

    /// Duration from intent received to confirmation (ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
}

impl ExecutionResult {
    pub fn new_sent(
        component: &str,
        build: &str,
        run_id: &str,
        execution_id: String,
        decision_id: String,
        intent_id: String,
        signature: Option<String>,
        bundle_id: Option<String>,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            execution_id,
            decision_id,
            intent_id,
            signature,
            bundle_id,
            status: ExecutionStatus::Sent,
            confirmed_slot: None,
            fees: None,
            pnl: None,
            error_message: None,
            latency_ms: None,
        }
    }

    pub fn mark_confirmed(
        mut self,
        slot: u64,
        fees: ExecutionFees,
        pnl: ExecutionPnl,
        latency_ms: u64,
    ) -> Self {
        self.status = ExecutionStatus::Confirmed;
        self.confirmed_slot = Some(slot);
        self.fees = Some(fees);
        self.pnl = Some(pnl);
        self.latency_ms = Some(latency_ms);
        self
    }

    pub fn mark_failed(mut self, error: String) -> Self {
        self.status = ExecutionStatus::Failed;
        self.error_message = Some(error);
        self
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explicit_amount() {
        let amt = ExplicitAmount::new(1_000_000_000, 9); // 1 SOL
        assert_eq!(amt.raw, 1_000_000_000);
        assert_eq!(amt.decimals, 9);
        assert_eq!(amt.ui, Some(Decimal::from(1)));
    }

    #[test]
    fn test_market_event_serialization() {
        let event = MarketEvent::new(
            "market-data",
            "v0.1.0",
            "run-123",
            "evt-001".to_string(),
            "geyser",
            Some(12345),
            MarketEventKind::SlotUpdate { current_slot: 12345 },
        );

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("schema_version"));
        assert!(json.contains("event_id"));
        assert!(json.contains("SlotUpdate"));

        let parsed: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.event_id, "evt-001");
    }

    #[test]
    fn test_trade_intent_serialization() {
        let intent = TradeIntent::new(
            "momentum-bot",
            "v0.1.0",
            "run-456",
            "intent-001".to_string(),
            "momentum-bot",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(100_000_000, 9), // 0.1 SOL
            TradeResources {
                input_mint: "So11111111111111111111111111111111111111112".to_string(),
                output_mint: "TokenMint123".to_string(),
                pools: vec!["Pool123".to_string()],
                accounts: vec![],
            },
            50, // 0.5% expected ROI
            100, // 1% max slippage
            TradeSide::Buy,
            TradingRegime::Early,
        );

        let json = serde_json::to_string(&intent).unwrap();
        assert!(json.contains("intent_id"));
        assert!(json.contains("required_capital"));
        assert!(json.contains("StrategyA"));

        let parsed: TradeIntent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.intent_id, "intent-001");
        assert_eq!(parsed.tier, IntentTier::Tier1);
    }

    #[test]
    fn test_decision_record_rejected() {
        let checks = vec![
            CheckResult {
                check_name: "ttl_valid".to_string(),
                passed: true,
                reason_code: None,
                details: None,
            },
            CheckResult {
                check_name: "risk_limit".to_string(),
                passed: false,
                reason_code: Some("RISK_LIMIT_EXCEEDED".to_string()),
                details: Some("Daily loss limit reached".to_string()),
            },
        ];

        let record = DecisionRecord::new_rejected(
            "execution-engine",
            "v0.1.0",
            "run-789",
            "dec-001".to_string(),
            "intent-001".to_string(),
            IntentOrigin::StrategyA,
            TradingRegime::Early,
            checks,
            "RISK_LIMIT_EXCEEDED".to_string(),
        );

        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("decision_id"));
        assert!(json.contains("RISK_LIMIT_EXCEEDED"));
        assert!(json.contains("Rejected"));

        let parsed: DecisionRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.outcome, DecisionOutcome::Rejected);
    }

    #[test]
    fn test_execution_result_serialization() {
        let result = ExecutionResult::new_sent(
            "execution-engine",
            "v0.1.0",
            "run-789",
            "exec-001".to_string(),
            "dec-001".to_string(),
            "intent-001".to_string(),
            Some("5abc123...".to_string()),
            None,
        );

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("execution_id"));
        assert!(json.contains("Sent"));

        let parsed: ExecutionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.status, ExecutionStatus::Sent);
    }
}
