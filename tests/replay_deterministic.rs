use base64::Engine;
use ironcrab::backtest::replay::{CfmPoolJson, ReplayConfig, ReplayStore, TraceEvent};

#[test]
fn replay_builds_slot_log_and_account_events() {
    // Build a small synthetic trace
    let mut events = Vec::new();
    events.push(TraceEvent::Slot { slot: 1 });
    events.push(TraceEvent::Slot { slot: 2 });
    events.push(TraceEvent::Slot { slot: 3 });
    events.push(TraceEvent::Log {
        slot: 2,
        msg: "hello".into(),
    });
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
