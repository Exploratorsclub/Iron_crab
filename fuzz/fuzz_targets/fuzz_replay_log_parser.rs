#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Try to parse input as UTF-8 JSONL with TraceEvent lines and exercise load_trace/from_events paths.
    if let Ok(s) = std::str::from_utf8(data) {
        // Build a temp in-memory JSONL string trying a couple of shapes
        let mut lines = String::new();
        for chunk in s.split('\n').take(16) {
            let t = chunk.trim();
            if t.is_empty() { continue; }
            // Randomly choose a variant by length heuristics
            let ev = if t.len() % 3 == 0 {
                format!("{{\"Slot\":{{\"slot\":{}}}}}", (t.len() % 10_000) as u64)
            } else if t.len() % 3 == 1 {
                // base64 data may be garbage; function should ignore decode failures
                format!("{{\"Account\":{{\"pubkey\":\"{}\",\"data_b64\":\"{}\"}}}}", &t.chars().take(10).collect::<String>(), base64::encode(t))
            } else {
                format!("{{\"Log\":{{\"slot\":{},\"msg\":\"{}\"}}}}", (t.len() % 10_000) as u64, t.replace('"', ""))
            };
            lines.push_str(&ev);
            lines.push('\n');
        }
        let tmp = tempfile::NamedTempFile::new();
        if let Ok(mut f) = tmp.and_then(|t| t.keep()) {
            let _ = std::fs::write(&f.0, lines);
            let path = f.0.to_string_lossy().to_string();
            let cfg = ironcrab::backtest::replay::ReplayConfig { start_slot: 0, end_slot: 10, speedup: None, trace_path: Some(path.clone()), slot_ms: Some(400), seed: None };
            let _ = ironcrab::backtest::replay::build_events_from_trace(&cfg);
            let _ = std::fs::remove_file(path);
        }
    }
});
