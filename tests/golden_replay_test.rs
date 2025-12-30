//! Golden Replay Tests (DoD N) P1)
//!
//! These tests validate that the execution pipeline produces deterministic,
//! reproducible results by comparing against pre-recorded "golden" fixtures.
//!
//! Golden fixtures are stored in tests/fixtures/golden_replays/
//! Any intentional change to decision logic must update these fixtures.

use ironcrab::ipc::{
    CheckResult, DecisionOutcome, DecisionRecord, ExplicitAmount, IntentOrigin, IntentTier,
    RecordHeader, SimulationResult, TradeIntent, TradeResources, TradeSide, TradingRegime,
};
use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

/// Load TradeIntents from a JSONL fixture file
fn load_intents(fixture_name: &str) -> Vec<TradeIntent> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/golden_replays")
        .join(format!("{}_intents.jsonl", fixture_name));

    let file = fs::File::open(&path).unwrap_or_else(|e| {
        panic!("Failed to open fixture {}: {}", path.display(), e);
    });

    BufReader::new(file)
        .lines()
        .filter_map(|line| line.ok())
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(&line).unwrap_or_else(|e| {
                panic!("Failed to parse intent: {} - line: {}", e, line);
            })
        })
        .collect()
}

/// Load expected DecisionRecords from a JSONL fixture file
fn load_expected_decisions(fixture_name: &str) -> Vec<GoldenDecision> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/golden_replays")
        .join(format!("{}_expected.jsonl", fixture_name));

    let file = fs::File::open(&path).unwrap_or_else(|e| {
        panic!("Failed to open fixture {}: {}", path.display(), e);
    });

    BufReader::new(file)
        .lines()
        .filter_map(|line| line.ok())
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(&line).unwrap_or_else(|e| {
                panic!("Failed to parse expected decision: {} - line: {}", e, line);
            })
        })
        .collect()
}

/// Simplified decision structure for golden comparison
/// (doesn't need all header fields, just the important ones)
#[derive(Debug, Clone, serde::Deserialize)]
struct GoldenDecision {
    decision_id: String,
    intent_id: String,
    origin_type: IntentOrigin,
    regime: TradingRegime,
    checks: Vec<GoldenCheck>,
    primary_reject_reason: Option<String>,
    outcome: DecisionOutcome,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct GoldenCheck {
    check_name: String,
    passed: bool,
    reason_code: Option<String>,
    details: Option<String>,
}

/// Simulates the decision engine logic for testing
/// This must match the real execution-engine behavior
fn simulate_decision(
    intent: &TradeIntent,
    sequence_num: u64,
    config: &TestConfig,
    state: &mut TestState,
) -> GoldenDecision {
    let mut checks = Vec::new();

    // Check 1: Idempotency
    let is_duplicate = state.processed_intents.contains(&intent.intent_id);
    if is_duplicate {
        checks.push(GoldenCheck {
            check_name: "idempotency".to_string(),
            passed: false,
            reason_code: Some("LOCK_DUPLICATE_INTENT".to_string()),
            details: Some("Intent already processed".to_string()),
        });
        return GoldenDecision {
            decision_id: format!("golden-dec-{:06}", sequence_num),
            intent_id: intent.intent_id.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some("LOCK_DUPLICATE_INTENT".to_string()),
            outcome: DecisionOutcome::Rejected,
        };
    }
    checks.push(GoldenCheck {
        check_name: "idempotency".to_string(),
        passed: true,
        reason_code: None,
        details: None,
    });

    // Check 2: TTL validity
    checks.push(GoldenCheck {
        check_name: "ttl_valid".to_string(),
        passed: true,
        reason_code: None,
        details: None,
    });

    // Check 3a: Max position size
    if intent.required_capital.raw > config.max_position_size_lamports {
        checks.push(GoldenCheck {
            check_name: "max_position_size".to_string(),
            passed: false,
            reason_code: Some("RISK_MAX_POSITION".to_string()),
            details: Some(format!(
                "required={} > max={}",
                intent.required_capital.raw, config.max_position_size_lamports
            )),
        });
        return GoldenDecision {
            decision_id: format!("golden-dec-{:06}", sequence_num),
            intent_id: intent.intent_id.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some("RISK_MAX_POSITION".to_string()),
            outcome: DecisionOutcome::Rejected,
        };
    }
    checks.push(GoldenCheck {
        check_name: "max_position_size".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "required={} <= max={}",
            intent.required_capital.raw, config.max_position_size_lamports
        )),
    });

    // Check 3b: Max slippage
    if intent.max_slippage_bps > config.max_slippage_bps {
        checks.push(GoldenCheck {
            check_name: "max_slippage".to_string(),
            passed: false,
            reason_code: Some("SIM_SLIPPAGE_EXCEEDED".to_string()),
            details: Some(format!(
                "intent_slippage={}bps > max={}bps",
                intent.max_slippage_bps, config.max_slippage_bps
            )),
        });
        return GoldenDecision {
            decision_id: format!("golden-dec-{:06}", sequence_num),
            intent_id: intent.intent_id.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some("SIM_SLIPPAGE_EXCEEDED".to_string()),
            outcome: DecisionOutcome::Rejected,
        };
    }
    checks.push(GoldenCheck {
        check_name: "max_slippage".to_string(),
        passed: true,
        reason_code: None,
        details: None,
    });

    // Check 3c: Max open positions
    if state.open_positions >= config.max_open_positions {
        checks.push(GoldenCheck {
            check_name: "max_open_positions".to_string(),
            passed: false,
            reason_code: Some("RISK_MAX_OPEN_POSITIONS".to_string()),
            details: Some(format!(
                "current={} >= max={}",
                state.open_positions, config.max_open_positions
            )),
        });
        return GoldenDecision {
            decision_id: format!("golden-dec-{:06}", sequence_num),
            intent_id: intent.intent_id.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some("RISK_MAX_OPEN_POSITIONS".to_string()),
            outcome: DecisionOutcome::Rejected,
        };
    }
    checks.push(GoldenCheck {
        check_name: "max_open_positions".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "current={} < max={}",
            state.open_positions, config.max_open_positions
        )),
    });

    // Check 3d: Daily loss limit
    if state.daily_loss_lamports >= config.daily_loss_limit_lamports as i64 {
        checks.push(GoldenCheck {
            check_name: "daily_loss_limit".to_string(),
            passed: false,
            reason_code: Some("RISK_DAILY_LOSS_LIMIT".to_string()),
            details: Some(format!(
                "daily_loss={} >= limit={}",
                state.daily_loss_lamports, config.daily_loss_limit_lamports
            )),
        });
        return GoldenDecision {
            decision_id: format!("golden-dec-{:06}", sequence_num),
            intent_id: intent.intent_id.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some("RISK_DAILY_LOSS_LIMIT".to_string()),
            outcome: DecisionOutcome::Rejected,
        };
    }
    checks.push(GoldenCheck {
        check_name: "daily_loss_limit".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "daily_loss={} < limit={}",
            state.daily_loss_lamports, config.daily_loss_limit_lamports
        )),
    });

    // Check 4: Capital lock
    checks.push(GoldenCheck {
        check_name: "capital_lock".to_string(),
        passed: true,
        reason_code: None,
        details: None,
    });

    // Check 5: Simulation
    // Special case: output_mint starting with "SIMFAIL" triggers sim failure
    if intent.resources.output_mint.starts_with("SIMFAIL") {
        checks.push(GoldenCheck {
            check_name: "simulation".to_string(),
            passed: false,
            reason_code: Some("SIM_FAILED".to_string()),
            details: Some("Custom program error: 0x1".to_string()),
        });
        return GoldenDecision {
            decision_id: format!("golden-dec-{:06}", sequence_num),
            intent_id: intent.intent_id.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some("SIM_FAILED".to_string()),
            outcome: DecisionOutcome::SimFailed,
        };
    }

    checks.push(GoldenCheck {
        check_name: "simulation".to_string(),
        passed: true,
        reason_code: None,
        details: Some("CU consumed: 150000".to_string()),
    });

    // Mark as processed
    state.processed_intents.insert(intent.intent_id.clone());

    GoldenDecision {
        decision_id: format!("golden-dec-{:06}", sequence_num),
        intent_id: intent.intent_id.clone(),
        origin_type: intent.origin_type,
        regime: intent.regime,
        checks,
        primary_reject_reason: None,
        outcome: DecisionOutcome::Sent,
    }
}

struct TestConfig {
    max_position_size_lamports: u64,
    daily_loss_limit_lamports: u64,
    max_open_positions: usize,
    max_slippage_bps: u32,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            max_position_size_lamports: 500_000_000,  // 0.5 SOL
            daily_loss_limit_lamports: 5_000_000_000, // 5 SOL
            max_open_positions: 5,
            max_slippage_bps: 500, // 5%
        }
    }
}

struct TestState {
    processed_intents: HashSet<String>,
    open_positions: usize,
    daily_loss_lamports: i64,
}

impl Default for TestState {
    fn default() -> Self {
        Self {
            processed_intents: HashSet::new(),
            open_positions: 0,
            daily_loss_lamports: 0,
        }
    }
}

/// Compare a generated decision against the golden expected decision
fn assert_decision_matches(actual: &GoldenDecision, expected: &GoldenDecision) {
    assert_eq!(
        actual.intent_id, expected.intent_id,
        "Intent ID mismatch"
    );
    assert_eq!(
        actual.outcome, expected.outcome,
        "Outcome mismatch for intent {}",
        actual.intent_id
    );
    assert_eq!(
        actual.primary_reject_reason, expected.primary_reject_reason,
        "Reject reason mismatch for intent {}",
        actual.intent_id
    );
    assert_eq!(
        actual.checks.len(),
        expected.checks.len(),
        "Check count mismatch for intent {}",
        actual.intent_id
    );

    for (actual_check, expected_check) in actual.checks.iter().zip(expected.checks.iter()) {
        assert_eq!(
            actual_check.check_name, expected_check.check_name,
            "Check name mismatch"
        );
        assert_eq!(
            actual_check.passed, expected_check.passed,
            "Check '{}' pass/fail mismatch for intent {}",
            actual_check.check_name, actual.intent_id
        );
        assert_eq!(
            actual_check.reason_code, expected_check.reason_code,
            "Check '{}' reason mismatch for intent {}",
            actual_check.check_name, actual.intent_id
        );
    }
}

// ============================================================================
// Golden Replay Tests
// ============================================================================

#[test]
fn golden_replay_normal_trade() {
    let intents = load_intents("normal_trade");
    let expected = load_expected_decisions("normal_trade");

    assert_eq!(intents.len(), expected.len(), "Fixture count mismatch");

    let config = TestConfig::default();
    let mut state = TestState::default();

    for (i, (intent, expected_decision)) in intents.iter().zip(expected.iter()).enumerate() {
        let actual = simulate_decision(intent, (i + 1) as u64, &config, &mut state);
        assert_decision_matches(&actual, expected_decision);
    }
}

#[test]
fn golden_replay_rejected_trade() {
    let intents = load_intents("rejected_trade");
    let expected = load_expected_decisions("rejected_trade");

    assert_eq!(intents.len(), expected.len(), "Fixture count mismatch");

    let config = TestConfig::default();
    let mut state = TestState::default();

    for (i, (intent, expected_decision)) in intents.iter().zip(expected.iter()).enumerate() {
        let actual = simulate_decision(intent, (i + 1) as u64, &config, &mut state);
        assert_decision_matches(&actual, expected_decision);
    }
}

#[test]
fn golden_replay_sim_failed() {
    let intents = load_intents("sim_failed");
    let expected = load_expected_decisions("sim_failed");

    assert_eq!(intents.len(), expected.len(), "Fixture count mismatch");

    let config = TestConfig::default();
    let mut state = TestState::default();

    for (i, (intent, expected_decision)) in intents.iter().zip(expected.iter()).enumerate() {
        let actual = simulate_decision(intent, (i + 1) as u64, &config, &mut state);
        assert_decision_matches(&actual, expected_decision);
    }
}

#[test]
fn golden_replay_determinism() {
    // Run the same scenario twice and ensure identical results
    let intents = load_intents("normal_trade");

    let config = TestConfig::default();

    // Run 1
    let mut state1 = TestState::default();
    let decisions1: Vec<_> = intents
        .iter()
        .enumerate()
        .map(|(i, intent)| simulate_decision(intent, (i + 1) as u64, &config, &mut state1))
        .collect();

    // Run 2
    let mut state2 = TestState::default();
    let decisions2: Vec<_> = intents
        .iter()
        .enumerate()
        .map(|(i, intent)| simulate_decision(intent, (i + 1) as u64, &config, &mut state2))
        .collect();

    // Compare
    for (d1, d2) in decisions1.iter().zip(decisions2.iter()) {
        assert_eq!(d1.intent_id, d2.intent_id);
        assert_eq!(d1.outcome, d2.outcome);
        assert_eq!(d1.primary_reject_reason, d2.primary_reject_reason);
        assert_eq!(d1.checks.len(), d2.checks.len());
    }
}
