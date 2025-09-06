//! Deterministic replay scaffolding (slot iterator + trace-backed RPC mocks)
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    pub start_slot: u64,
    pub end_slot: u64,
    pub speedup: Option<f64>,
    pub trace_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceEvent {
    Slot { slot: u64 },
    Account { pubkey: String, data_b64: String },
    Log { slot: u64, msg: String },
}

pub struct SlotIterator { cur: u64, end: u64 }
impl SlotIterator {
    pub fn new(start:u64, end:u64) -> Self { Self { cur:start, end } }
    pub fn next_slot(&mut self) -> Option<u64> { if self.cur>self.end { None } else { let s=self.cur; self.cur+=1; Some(s) } }
}

pub fn load_trace(path:&str) -> anyhow::Result<Vec<TraceEvent>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    // Try JSONL first (one event per line)
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut events: Vec<TraceEvent> = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let s = line.trim();
        if s.is_empty() { continue; }
        if let Ok(ev) = serde_json::from_str::<TraceEvent>(s) { events.push(ev); }
    }
    if !events.is_empty() { return Ok(events); }
    // Fallback: full JSON array
    let text = std::fs::read_to_string(path)?;
    let vec: Vec<TraceEvent> = serde_json::from_str(&text)?;
    Ok(vec)
}
