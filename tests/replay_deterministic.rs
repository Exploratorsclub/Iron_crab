use base64::Engine;
use ironcrab::backtest::replay::{CfmPoolJson, ReplayConfig, ReplayStore, TraceEvent};

#[test]
fn replay_builds_slot_log_and_account_events() {
    // Build a small synthetic trace
    let mut events = vec![
        TraceEvent::Slot { slot: 1 },
        TraceEvent::Slot { slot: 2 },
        TraceEvent::Slot { slot: 3 },
        TraceEvent::Log {
            slot: 2,
            msg: "hello".into(),
        },
    ];
    let pool = CfmPoolJson {
        pool: "P".into(),
        base_mint: "A".into(),
        quote_mint: "B".into(),
        base_reserve: 1_000_000,
        quote_reserve: 2_000_000,
        fee_bps: 30,
    };
    let json = serde_json::to_string(&pool).unwrap();
    let data_b64 = base64::engine::general_purpose::STANDARD.encode(json.as_bytes());
    events.push(TraceEvent::Account {
        pubkey: "X".into(),
        data_b64,
    });

    // Build store and events
    let store = ReplayStore::from_events(&events);
    let cfg = ReplayConfig {
        start_slot: 1,
        end_slot: 3,
        speedup: None,
        trace_path: None,
        slot_ms: Some(400),
        seed: Some(42),
    };
    let sim = store.to_sim_events(&cfg);

    // Expect 3 SlotAdvance + 1 Log + 2 account-derived events (NewPool + CfmPriceUpdate)
    assert_eq!(sim.len(), 6);

    // Check that SlotAdvance events have ts 400, 800, 1200 (slot_ms = 400)
    let mut slot_ts: Vec<u64> = sim
        .iter()
        .filter_map(|e| match e.kind {
            ironcrab::backtest::types::SimEventKind::SlotAdvance { .. } => Some(e.ts_ms),
            _ => None,
        })
        .collect();
    slot_ts.sort();
    assert_eq!(slot_ts, vec![400, 800, 1200]);
}

// ============================================================================
// Decision Record Replay Tests
// ============================================================================

use ironcrab::ipc::{
    CheckResult, DecisionOutcome, DecisionRecord, ExplicitAmount, IntentOrigin, IntentTier,
    RecordHeader, SimulationResult, TradeIntent, TradeResources, TradeSide, TradingRegime,
};
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use tempfile::tempdir;

/// Deterministic "decision engine" for testing
/// Given the same inputs, must produce the same outputs
fn make_decision(intent: &TradeIntent, sequence_num: u64) -> DecisionRecord {
    let mut checks = Vec::new();

    // Check 1: TTL validity
    let ttl_valid = intent.ttl_ms.map(|t| t > 0).unwrap_or(true);
    checks.push(CheckResult {
        check_name: "ttl_valid".to_string(),
        passed: ttl_valid,
        reason_code: if ttl_valid {
            None
        } else {
            Some("TTL_EXPIRED".to_string())
        },
        details: None,
    });

    // Check 2: Capital check (deterministic based on amount)
    let capital_ok = intent.required_capital.raw <= 100_000_000; // 0.1 SOL limit
    checks.push(CheckResult {
        check_name: "capital_available".to_string(),
        passed: capital_ok,
        reason_code: if capital_ok {
            None
        } else {
            Some("INSUFFICIENT_CAPITAL".to_string())
        },
        details: Some(format!("requested={}", intent.required_capital.raw)),
    });

    // Check 3: Slippage check
    let slippage_ok = intent.max_slippage_bps <= 500;
    checks.push(CheckResult {
        check_name: "slippage_limit".to_string(),
        passed: slippage_ok,
        reason_code: if slippage_ok {
            None
        } else {
            Some("SLIPPAGE_TOO_HIGH".to_string())
        },
        details: None,
    });

    let all_passed = checks.iter().all(|c| c.passed);

    let (outcome, reject_reason, sim_result) = if !all_passed {
        let first_fail = checks.iter().find(|c| !c.passed).unwrap();
        (
            DecisionOutcome::Rejected,
            Some(
                first_fail
                    .reason_code
                    .clone()
                    .unwrap_or("UNKNOWN".to_string()),
            ),
            None,
        )
    } else {
        let sim = SimulationResult {
            success: true,
            error_code: None,
            logs_preview: Some(format!("Simulated at seq {}", sequence_num)),
            compute_units_consumed: Some(150_000 + (sequence_num * 100) as u64),
        };
        (DecisionOutcome::Sent, None, Some(sim))
    };

    DecisionRecord {
        header: RecordHeader::new("replay-test", "0.1.0", "test-run-123"),
        decision_id: format!("dec-{:06}", sequence_num),
        intent_id: intent.intent_id.clone(),
        origin_type: intent.origin_type,
        regime: intent.regime,
        checks,
        primary_reject_reason: reject_reason,
        plan_hash: Some(format!("plan-{}", sequence_num)),
        simulate: sim_result,
        send: None,
        outcome,
        config_snapshot_id: Some("config-v1".to_string()),
        input_snapshots: HashMap::new(),
    }
}

/// Create a test intent with deterministic properties
fn create_test_intent(seq: u64, regime: TradingRegime, capital_lamports: u64) -> TradeIntent {
    TradeIntent::new(
        "replay-test",
        "0.1.0",
        "test-run-123",
        format!("intent-{:06}", seq),
        "test-harness",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(capital_lamports, 9),
        TradeResources {
            input_mint: "So11111111111111111111111111111111111111112".to_string(),
            output_mint: format!("TestToken{:03}", seq % 100),
            pools: vec![format!("Pool{:03}", seq % 50)],
            accounts: vec![],
        },
        100,
        200,
        TradeSide::Buy,
        regime,
    )
    .with_ttl_ms(5000)
}

/// Read DecisionRecords from a JSONL file
fn read_decisions_from_jsonl(path: &std::path::Path) -> Vec<DecisionRecord> {
    let file = fs::File::open(path).expect("Failed to open JSONL file");
    let reader = BufReader::new(file);

    reader
        .lines()
        .filter_map(|line| line.ok())
        .filter_map(|line| serde_json::from_str(&line).ok())
        .collect()
}

#[test]
fn test_decision_record_roundtrip() {
    let dir = tempdir().unwrap();
    let config = JsonlWriterConfig::new("decision_records")
        .with_log_dir(dir.path())
        .with_flush_each_write(true);

    let writer = JsonlWriter::new(config).unwrap();

    let test_cases = vec![
        (1, TradingRegime::Early, 50_000_000),
        (2, TradingRegime::Early, 200_000_000),
        (3, TradingRegime::Established, 10_000_000),
        (4, TradingRegime::Early, 80_000_000),
        (5, TradingRegime::Established, 500_000_000),
    ];

    let mut original_decisions: Vec<DecisionRecord> = Vec::new();

    for (seq, regime, capital) in &test_cases {
        let intent = create_test_intent(*seq, *regime, *capital);
        let decision = make_decision(&intent, *seq);
        writer.write(&decision).expect("Failed to write decision");
        original_decisions.push(decision);
    }

    writer.flush().unwrap();

    let path = writer.current_path().unwrap();
    let loaded_decisions = read_decisions_from_jsonl(&path);

    assert_eq!(
        loaded_decisions.len(),
        original_decisions.len(),
        "Should have same number of decisions"
    );

    for (original, loaded) in original_decisions.iter().zip(loaded_decisions.iter()) {
        assert_eq!(original.decision_id, loaded.decision_id);
        assert_eq!(original.intent_id, loaded.intent_id);
        assert_eq!(original.outcome, loaded.outcome);
        assert_eq!(original.checks.len(), loaded.checks.len());
        assert_eq!(
            original.primary_reject_reason,
            loaded.primary_reject_reason
        );
    }
}

#[test]
fn test_replay_determinism() {
    let test_cases = vec![
        (1, TradingRegime::Early, 50_000_000),
        (2, TradingRegime::Early, 200_000_000),
        (3, TradingRegime::Established, 10_000_000),
        (4, TradingRegime::Early, 80_000_000),
        (5, TradingRegime::Established, 500_000_000),
        (6, TradingRegime::Early, 99_000_000),
        (7, TradingRegime::Early, 101_000_000),
    ];

    // Run 1
    let decisions_run1: Vec<_> = test_cases
        .iter()
        .map(|(seq, regime, capital)| {
            let intent = create_test_intent(*seq, *regime, *capital);
            make_decision(&intent, *seq)
        })
        .collect();

    // Run 2 (replay)
    let decisions_run2: Vec<_> = test_cases
        .iter()
        .map(|(seq, regime, capital)| {
            let intent = create_test_intent(*seq, *regime, *capital);
            make_decision(&intent, *seq)
        })
        .collect();

    assert_eq!(decisions_run1.len(), decisions_run2.len());

    for (d1, d2) in decisions_run1.iter().zip(decisions_run2.iter()) {
        assert_eq!(d1.decision_id, d2.decision_id);
        assert_eq!(d1.outcome, d2.outcome);
        assert_eq!(d1.primary_reject_reason, d2.primary_reject_reason);
        assert_eq!(d1.plan_hash, d2.plan_hash);

        match (&d1.simulate, &d2.simulate) {
            (Some(s1), Some(s2)) => {
                assert_eq!(s1.success, s2.success);
                assert_eq!(s1.compute_units_consumed, s2.compute_units_consumed);
            }
            (None, None) => {}
            _ => panic!("Simulation result mismatch for {}", d1.decision_id),
        }
    }
}

#[test]
fn test_decision_outcomes_correct() {
    // Should pass: under capital limit
    let intent1 = create_test_intent(1, TradingRegime::Early, 50_000_000);
    let decision1 = make_decision(&intent1, 1);
    assert_eq!(decision1.outcome, DecisionOutcome::Sent);

    // Should fail: over capital limit
    let intent2 = create_test_intent(2, TradingRegime::Early, 200_000_000);
    let decision2 = make_decision(&intent2, 2);
    assert_eq!(decision2.outcome, DecisionOutcome::Rejected);
    assert!(decision2
        .primary_reject_reason
        .as_ref()
        .unwrap()
        .contains("CAPITAL"));

    // Edge case: exactly at limit
    let intent3 = create_test_intent(3, TradingRegime::Early, 100_000_000);
    let decision3 = make_decision(&intent3, 3);
    assert_eq!(decision3.outcome, DecisionOutcome::Sent);

    // Edge case: just over limit
    let intent4 = create_test_intent(4, TradingRegime::Early, 100_000_001);
    let decision4 = make_decision(&intent4, 4);
    assert_eq!(decision4.outcome, DecisionOutcome::Rejected);
}

#[test]
fn test_jsonl_append_integrity() {
    let dir = tempdir().unwrap();
    let config = JsonlWriterConfig::new("append_test")
        .with_log_dir(dir.path())
        .with_flush_each_write(true);

    let writer = JsonlWriter::new(config).unwrap();

    // Write in batches
    for batch in 0..3 {
        for i in 0..5 {
            let seq = batch * 5 + i;
            let intent = create_test_intent(seq, TradingRegime::Early, 50_000_000);
            let decision = make_decision(&intent, seq);
            writer.write(&decision).unwrap();
        }
        writer.flush().unwrap();
    }

    let path = writer.current_path().unwrap();
    let loaded = read_decisions_from_jsonl(&path);

    assert_eq!(loaded.len(), 15, "Should have 15 records (3 batches × 5)");

    for (i, decision) in loaded.iter().enumerate() {
        let expected_id = format!("dec-{:06}", i);
        assert_eq!(decision.decision_id, expected_id);
    }
}
