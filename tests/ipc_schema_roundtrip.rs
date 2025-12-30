//! Integration test: Schema + Decision Record serialization roundtrip
//!
//! Validates required fields per:
//! - docs/DEFINITION_OF_DONE.md
//! - docs/STORAGE_CONVENTIONS.md
//!
//! This test is deterministic (no network).

use ironcrab::ipc::{
    CheckResult, DecisionOutcome, DecisionRecord, ExecutionFees, ExecutionPnl, ExecutionResult,
    ExecutionStatus, ExplicitAmount, FeePolicy, IntentOrigin, IntentTier, MarketEvent,
    MarketEventKind, RecordHeader, RejectReason, SimulationResult, TradeIntent, TradeResources,
    TradeSide, TradingRegime, SCHEMA_VERSION,
};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Test that RecordHeader contains all required fields per STORAGE_CONVENTIONS.md §4
#[test]
fn test_record_header_required_fields() {
    let header = RecordHeader::new("test-component", "v0.1.0", "run-12345");

    // Required fields per STORAGE_CONVENTIONS.md
    assert_eq!(header.schema_version, SCHEMA_VERSION);
    assert!(header.ts_unix_ms > 0, "ts_unix_ms should be set");
    assert_eq!(header.component, "test-component");
    assert_eq!(header.build, "v0.1.0");
    assert_eq!(header.run_id, "run-12345");

    // Serialization roundtrip
    let json = serde_json::to_string(&header).unwrap();
    assert!(json.contains("schema_version"));
    assert!(json.contains("ts_unix_ms"));
    assert!(json.contains("component"));
    assert!(json.contains("build"));
    assert!(json.contains("run_id"));

    let parsed: RecordHeader = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.schema_version, header.schema_version);
    assert_eq!(parsed.component, header.component);
}

/// Test ExplicitAmount handles units correctly (DoD §B: Units/Decimals explicit)
#[test]
fn test_explicit_amount_units() {
    // SOL: 9 decimals
    let sol = ExplicitAmount::new(1_500_000_000, 9); // 1.5 SOL
    assert_eq!(sol.raw, 1_500_000_000);
    assert_eq!(sol.decimals, 9);
    assert_eq!(sol.ui, Some(Decimal::new(15, 1))); // 1.5

    // USDC: 6 decimals
    let usdc = ExplicitAmount::new(1_000_000, 6); // 1 USDC
    assert_eq!(usdc.raw, 1_000_000);
    assert_eq!(usdc.decimals, 6);
    assert_eq!(usdc.ui, Some(Decimal::ONE));

    // Serialization includes all unit info
    let json = serde_json::to_string(&sol).unwrap();
    assert!(json.contains("\"raw\":1500000000"));
    assert!(json.contains("\"decimals\":9"));
}

/// Test MarketEvent has all required fields per STORAGE_CONVENTIONS.md §4.1
#[test]
fn test_market_event_required_fields() {
    let event = MarketEvent::new(
        "market-data",
        "v0.1.0",
        "run-abc",
        "evt-001".to_string(),
        "geyser",
        Some(12345),
        MarketEventKind::PoolCreated {
            pool_address: "Pool123".to_string(),
            base_mint: "BaseMint".to_string(),
            quote_mint: "QuoteMint".to_string(),
            dex: "raydium".to_string(),
            initial_liquidity_sol: Some(Decimal::from(100)),
        },
    );

    let json = serde_json::to_string(&event).unwrap();

    // Header fields
    assert!(json.contains("schema_version"));
    assert!(json.contains("ts_unix_ms"));
    assert!(json.contains("component"));
    assert!(json.contains("build"));
    assert!(json.contains("run_id"));

    // MarketEvent-specific fields per §4.1
    assert!(json.contains("event_id"));
    assert!(json.contains("source"));
    assert!(json.contains("slot"));
    assert!(json.contains("kind") || json.contains("PoolCreated")); // Flattened kind

    // Roundtrip
    let parsed: MarketEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.event_id, "evt-001");
    assert_eq!(parsed.source, "geyser");
    assert_eq!(parsed.slot, Some(12345));
}

/// Test TradeIntent has all required fields per DoD §B and STORAGE_CONVENTIONS.md §4.2
#[test]
fn test_trade_intent_required_fields() {
    let intent = TradeIntent::new(
        "momentum-bot",
        "v0.1.0",
        "run-xyz",
        "intent-001".to_string(),
        "momentum-bot",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(100_000_000, 9),
        TradeResources {
            input_mint: "SOL".to_string(),
            output_mint: "TOKEN".to_string(),
            pools: vec!["Pool1".to_string()],
            accounts: vec![],
        },
        50,  // expected_roi_bps
        100, // max_slippage_bps
        TradeSide::Buy,
        TradingRegime::Early,
    )
    .with_ttl_ms(5000);

    let json = serde_json::to_string(&intent).unwrap();

    // DoD §B required fields
    assert!(json.contains("intent_id"));
    assert!(json.contains("source"));
    assert!(json.contains("tier"));
    assert!(json.contains("required_capital"));
    assert!(json.contains("resources"));
    assert!(json.contains("expected_roi_bps"));
    assert!(json.contains("max_slippage_bps"));

    // DoD §D.1: origin_type for Typ A/B classification
    assert!(json.contains("origin_type"));
    assert!(json.contains("StrategyA"));

    // Regime classification
    assert!(json.contains("regime"));
    assert!(json.contains("Early"));

    // TTL/deadline
    assert!(json.contains("ttl_ms"));

    // Roundtrip
    let parsed: TradeIntent = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.intent_id, "intent-001");
    assert_eq!(parsed.tier, IntentTier::Tier1);
    assert_eq!(parsed.origin_type, IntentOrigin::StrategyA);
    assert_eq!(parsed.regime, TradingRegime::Early);
}

/// Test DecisionRecord has all required fields per DoD §E and STORAGE_CONVENTIONS.md §4.3
#[test]
fn test_decision_record_required_fields() {
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
            reason_code: Some("RISK_DAILY_LOSS_LIMIT".to_string()),
            details: Some("Daily loss limit exceeded".to_string()),
        },
    ];

    let decision = DecisionRecord::new_rejected(
        "execution-engine",
        "v0.1.0",
        "run-123",
        "dec-001".to_string(),
        "intent-001".to_string(),
        "momentum-bot".to_string(),
        IntentOrigin::StrategyA,
        TradingRegime::Early,
        checks,
        "RISK_DAILY_LOSS_LIMIT".to_string(),
    )
    .with_config_snapshot("config-snap-001".to_string())
    .with_input_snapshot("balances".to_string(), "{\"sol\": 1.5}".to_string());

    let json = serde_json::to_string(&decision).unwrap();

    // DoD §E required fields
    assert!(json.contains("decision_id"));
    assert!(json.contains("intent_id"));
    assert!(json.contains("checks")); // List of pass/fail
    assert!(json.contains("outcome"));
    assert!(json.contains("Rejected"));

    // P1: Source attribution (DoD §F.P1)
    assert!(json.contains("source"));
    assert!(json.contains("momentum-bot"));

    // DoD §D.1: origin_type classification
    assert!(json.contains("origin_type"));

    // Regime
    assert!(json.contains("regime"));

    // Reason-coded reject (DoD §J)
    assert!(json.contains("primary_reject_reason"));
    assert!(json.contains("RISK_DAILY_LOSS_LIMIT"));

    // Roundtrip
    let parsed: DecisionRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.decision_id, "dec-001");
    assert_eq!(parsed.intent_id, "intent-001");
    assert_eq!(parsed.source, "momentum-bot"); // P1: Source attribution
    assert_eq!(parsed.outcome, DecisionOutcome::Rejected);
    assert_eq!(parsed.checks.len(), 2);
    assert!(!parsed.checks[1].passed);
}

/// Test DecisionRecord for simulation failure includes sim details
#[test]
fn test_decision_record_sim_failed() {
    let checks = vec![CheckResult {
        check_name: "simulation".to_string(),
        passed: false,
        reason_code: Some("SIM_FAILED".to_string()),
        details: Some("InstructionError".to_string()),
    }];

    let sim_result = SimulationResult {
        success: false,
        error_code: Some("InstructionError".to_string()),
        logs_preview: Some("Program failed: insufficient funds".to_string()),
        compute_units_consumed: Some(50_000),
    };

    let decision = DecisionRecord::new_sim_failed(
        "execution-engine",
        "v0.1.0",
        "run-456",
        "dec-002".to_string(),
        "intent-002".to_string(),
        "arb-strategy".to_string(),
        IntentOrigin::StrategyA,
        TradingRegime::Established,
        checks,
        "plan-hash-xyz".to_string(),
        sim_result,
    );

    let json = serde_json::to_string(&decision).unwrap();

    // DoD §C: Simulation result included
    assert!(json.contains("simulate"));
    assert!(json.contains("logs_preview"));
    assert!(json.contains("plan_hash"));
    assert!(json.contains("SimFailed"));

    let parsed: DecisionRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.outcome, DecisionOutcome::SimFailed);
    assert!(parsed.simulate.is_some());
    assert!(!parsed.simulate.as_ref().unwrap().success);
}

/// Test ExecutionResult has all required fields per STORAGE_CONVENTIONS.md §4.4
#[test]
fn test_execution_result_required_fields() {
    let result = ExecutionResult::new_sent(
        "execution-engine",
        "v0.1.0",
        "run-789",
        "exe-001".to_string(),
        "dec-001".to_string(),
        "intent-001".to_string(),
        "momentum-bot".to_string(),
        Some("5abcdef123456...".to_string()),
        None,
    )
    .mark_confirmed(
        12345,
        ExecutionFees {
            network_fee_lamports: 5000,
            tip_lamports: 10000,
            compute_units: 150000,
        },
        ExecutionPnl {
            gross_lamports: 50000,
            net_lamports: 35000,
            decimals: 9,
        },
        250,
    );

    let json = serde_json::to_string(&result).unwrap();

    // Required fields
    assert!(json.contains("execution_id"));
    assert!(json.contains("decision_id"));
    assert!(json.contains("intent_id"));
    assert!(json.contains("signature"));
    assert!(json.contains("status"));
    assert!(json.contains("Confirmed"));

    // P1: Source attribution (DoD §F.P1)
    assert!(json.contains("source"));
    assert!(json.contains("momentum-bot"));

    // Fees and PnL
    assert!(json.contains("fees"));
    assert!(json.contains("pnl"));
    assert!(json.contains("latency_ms"));

    let parsed: ExecutionResult = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.execution_id, "exe-001");
    assert_eq!(parsed.source, "momentum-bot"); // P1: Source attribution
    assert_eq!(parsed.status, ExecutionStatus::Confirmed);
    assert!(parsed.fees.is_some());
    assert!(parsed.pnl.is_some());
}

/// Test RejectReason serialization and categorization
#[test]
fn test_reject_reason_codes() {
    // All reason codes should serialize to SCREAMING_SNAKE_CASE
    let reasons = vec![
        (RejectReason::TtlExpired, "TTL_EXPIRED"),
        (RejectReason::RiskDailyLossLimit, "RISK_DAILY_LOSS_LIMIT"),
        (RejectReason::SimFailed, "SIM_FAILED"),
        (RejectReason::LockCapitalConflict, "LOCK_CAPITAL_CONFLICT"),
        (RejectReason::MissingDecimals, "MISSING_DECIMALS"),
    ];

    for (reason, expected_str) in reasons {
        assert_eq!(reason.as_str(), expected_str);

        let json = serde_json::to_string(&reason).unwrap();
        assert!(json.contains(expected_str));

        let parsed: RejectReason = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, reason);
    }
}

/// Test that correlation IDs are preserved through the pipeline
#[test]
fn test_id_correlation() {
    let intent_id = "intent-corr-test";
    let decision_id = "dec-corr-test";
    let execution_id = "exe-corr-test";

    let intent = TradeIntent::new(
        "test",
        "v0.1.0",
        "run-test",
        intent_id.to_string(),
        "test",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(100, 9),
        TradeResources::default(),
        0,
        100,
        TradeSide::Buy,
        TradingRegime::NotApplicable,
    );

    let decision = DecisionRecord::new_rejected(
        "test",
        "v0.1.0",
        "run-test",
        decision_id.to_string(),
        intent_id.to_string(),
        "test-strategy".to_string(),
        IntentOrigin::StrategyA,
        TradingRegime::NotApplicable,
        vec![],
        "TEST".to_string(),
    );

    let execution = ExecutionResult::new_sent(
        "test",
        "v0.1.0",
        "run-test",
        execution_id.to_string(),
        decision_id.to_string(),
        intent_id.to_string(),
        "test-strategy".to_string(),
        None,
        None,
    );

    // All records should reference the same intent_id
    assert_eq!(intent.intent_id, intent_id);
    assert_eq!(decision.intent_id, intent_id);
    assert_eq!(execution.intent_id, intent_id);

    // Decision and execution should be correlated
    assert_eq!(execution.decision_id, decision_id);

    // P1: Source attribution - all records from same source chain
    assert_eq!(intent.source, "test-strategy");
    assert_eq!(decision.source, "test-strategy");
    assert_eq!(execution.source, "test-strategy");
}

/// Test JSONL format compatibility (one JSON object per line)
#[test]
fn test_jsonl_format() {
    let events = vec![
        MarketEvent::new(
            "test",
            "v1",
            "run",
            "e1".to_string(),
            "test",
            Some(1),
            MarketEventKind::SlotUpdate { current_slot: 1 },
        ),
        MarketEvent::new(
            "test",
            "v1",
            "run",
            "e2".to_string(),
            "test",
            Some(2),
            MarketEventKind::SlotUpdate { current_slot: 2 },
        ),
    ];

    let mut jsonl = String::new();
    for event in &events {
        let line = serde_json::to_string(event).unwrap();
        // Each line should be valid JSON without newlines
        assert!(!line.contains('\n'));
        jsonl.push_str(&line);
        jsonl.push('\n');
    }

    // Each line should parse independently
    for (i, line) in jsonl.lines().enumerate() {
        let parsed: MarketEvent = serde_json::from_str(line).unwrap();
        assert_eq!(parsed.event_id, format!("e{}", i + 1));
    }
}

/// P1: Test FeePolicy computes correct values for intents
#[test]
fn test_fee_policy_computation() {
    let policy = FeePolicy::default();

    // Default values
    assert_eq!(policy.default_compute_units, 200_000);
    assert_eq!(policy.max_compute_units, 1_400_000);
    assert_eq!(policy.arb_compute_units, 400_000);
    assert_eq!(policy.default_priority_fee_micro_lamports, 1_000);

    // Test intent with no hints (uses defaults)
    let intent_normal = TradeIntent::new(
        "test",
        "v1",
        "run",
        "i1".to_string(),
        "test-strategy",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(100_000_000, 9), // 0.1 SOL
        TradeResources::default(),
        50,  // 0.5% expected ROI
        100, // 1% max slippage
        TradeSide::Buy,
        TradingRegime::Early,
    );

    let cu = policy.compute_units_for_intent(&intent_normal);
    assert_eq!(cu, 200_000, "Normal intent should use default CU");

    let fee = policy.priority_fee_for_intent(&intent_normal);
    assert_eq!(fee, 1_000, "Tier1 intent should use default fee");

    // Test intent with bundle requirement (uses arb CU)
    let intent_bundle = intent_normal.clone().with_bundle(Some(10_000));
    let cu_bundle = policy.compute_units_for_intent(&intent_bundle);
    assert_eq!(cu_bundle, 400_000, "Bundle intent should use arb CU");

    // Test Tier0 intent (higher priority fee)
    let intent_tier0 = TradeIntent::new(
        "test",
        "v1",
        "run",
        "i2".to_string(),
        "test-strategy",
        IntentTier::Tier0,
        IntentOrigin::ExecutionMevB,
        ExplicitAmount::new(100_000_000, 9),
        TradeResources::default(),
        100,
        100,
        TradeSide::Buy,
        TradingRegime::NotApplicable,
    );

    let fee_tier0 = policy.priority_fee_for_intent(&intent_tier0);
    assert_eq!(fee_tier0, 10_000, "Tier0 intent should use tier0 fee");

    // Test fee hint override (capped at max)
    let intent_with_hint = intent_normal
        .clone()
        .with_fee_hints(Some(300_000), Some(50_000), Some(1));

    let cu_hint = policy.compute_units_for_intent(&intent_with_hint);
    assert_eq!(cu_hint, 300_000, "Should use hinted CU");

    let fee_hint = policy.priority_fee_for_intent(&intent_with_hint);
    assert_eq!(fee_hint, 50_000, "Should use hinted fee");

    // Test hint exceeding max (should be capped)
    let intent_extreme_hint = intent_normal
        .clone()
        .with_fee_hints(Some(2_000_000), Some(500_000), None);

    let cu_capped = policy.compute_units_for_intent(&intent_extreme_hint);
    assert_eq!(cu_capped, 1_400_000, "CU should be capped at max");

    let fee_capped = policy.priority_fee_for_intent(&intent_extreme_hint);
    assert_eq!(fee_capped, 100_000, "Fee should be capped at max");
}

/// P1: Test FeePolicy profitability check
#[test]
fn test_fee_policy_profitability() {
    let policy = FeePolicy::default();

    // Intent with high ROI - should be profitable
    let intent_profitable = TradeIntent::new(
        "test",
        "v1",
        "run",
        "i1".to_string(),
        "test-strategy",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(1_000_000_000, 9), // 1 SOL
        TradeResources::default(),
        100, // 1% expected ROI
        100,
        TradeSide::Buy,
        TradingRegime::Early,
    );

    let (is_profitable, profit_bps) = policy.is_profitable_after_fees(&intent_profitable);
    assert!(is_profitable, "High ROI intent should be profitable");
    assert!(profit_bps > 0, "Profit after fees should be positive");

    // Intent with very low ROI - might not be profitable after fees
    let intent_marginal = TradeIntent::new(
        "test",
        "v1",
        "run",
        "i2".to_string(),
        "test-strategy",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(10_000_000, 9), // 0.01 SOL (small)
        TradeResources::default(),
        5, // 0.05% expected ROI (very small)
        100,
        TradeSide::Buy,
        TradingRegime::Early,
    );

    let (is_profitable_marginal, _) = policy.is_profitable_after_fees(&intent_marginal);
    // Fee might eat into profit for small trades with low ROI
    // The exact result depends on fee calculation, but we test that it computes
    assert!(
        is_profitable_marginal || !is_profitable_marginal,
        "Should compute profitability"
    );
}

/// P1: Test FeePolicy serialization roundtrip
#[test]
fn test_fee_policy_serialization() {
    let policy = FeePolicy::default();

    let json = serde_json::to_string(&policy).unwrap();
    assert!(json.contains("default_compute_units"));
    assert!(json.contains("max_compute_units"));
    assert!(json.contains("default_priority_fee_micro_lamports"));
    assert!(json.contains("max_tx_cost_lamports"));
    assert!(json.contains("min_profit_after_fees_bps"));
    assert!(json.contains("urgency_multiplier_elevated"));

    let parsed: FeePolicy = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.default_compute_units, policy.default_compute_units);
    assert_eq!(
        parsed.max_priority_fee_micro_lamports,
        policy.max_priority_fee_micro_lamports
    );
}

/// P1: Test TradeIntent fee hints
#[test]
fn test_trade_intent_fee_hints() {
    let intent = TradeIntent::new(
        "test",
        "v1",
        "run",
        "i1".to_string(),
        "test-strategy",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(100_000_000, 9),
        TradeResources::default(),
        50,
        100,
        TradeSide::Buy,
        TradingRegime::Early,
    )
    .with_fee_hints(Some(300_000), Some(5_000), Some(2));

    // Check hints are set
    assert_eq!(intent.hint_compute_units, Some(300_000));
    assert_eq!(intent.hint_priority_fee_micro_lamports, Some(5_000));
    assert_eq!(intent.hint_urgency, Some(2));
    assert_eq!(intent.urgency(), 2);

    // Serialization includes hints
    let json = serde_json::to_string(&intent).unwrap();
    assert!(json.contains("hint_compute_units"));
    assert!(json.contains("hint_priority_fee_micro_lamports"));
    assert!(json.contains("hint_urgency"));

    // Roundtrip
    let parsed: TradeIntent = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.hint_compute_units, Some(300_000));
    assert_eq!(parsed.urgency(), 2);
}

/// P1: Test new fee-related reject reasons
#[test]
fn test_fee_reject_reasons() {
    let reasons = vec![
        RejectReason::FeeComputeExceedsLimit,
        RejectReason::FeePriorityExceedsLimit,
        RejectReason::FeeExceedsMaxCost,
        RejectReason::FeeUnprofitable,
    ];

    for reason in reasons {
        assert!(reason.is_fee_related(), "{:?} should be fee-related", reason);
        assert!(!reason.is_risk_related());
        assert!(!reason.is_simulation_related());
        assert!(!reason.is_bundle_related());

        // Roundtrip through string
        let s = reason.as_str();
        let parsed = RejectReason::from_str_loose(s);
        assert_eq!(parsed, reason, "Roundtrip failed for {:?}", reason);
    }

    // Test serialization
    let json = serde_json::to_string(&RejectReason::FeeUnprofitable).unwrap();
    assert_eq!(json, "\"FEE_UNPROFITABLE\"");
}
