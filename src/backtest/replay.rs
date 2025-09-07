//! Deterministic replay scaffolding (slot iterator + trace-backed RPC mocks)
use crate::backtest::types::{SimEvent, SimEventKind};
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    pub start_slot: u64,
    pub end_slot: u64,
    pub speedup: Option<f64>,
    pub trace_path: Option<String>,
    /// Deterministic slot duration mapping to milliseconds (default 400ms)
    pub slot_ms: Option<u64>,
    /// Optional deterministic RNG seed for future stochastic models
    pub seed: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceEvent {
    Slot {
        slot: u64,
    },
    /// Account snapshot/update. Data is base64; if it's UTF-8 JSON matching a known schema we can project it to SimEvents.
    Account {
        pubkey: String,
        data_b64: String,
    },
    Log {
        slot: u64,
        msg: String,
    },
}

pub struct SlotIterator {
    cur: u64,
    end: u64,
}
impl SlotIterator {
    pub fn new(start: u64, end: u64) -> Self {
        Self { cur: start, end }
    }
    pub fn next_slot(&mut self) -> Option<u64> {
        if self.cur > self.end {
            None
        } else {
            let s = self.cur;
            self.cur += 1;
            Some(s)
        }
    }
}

pub fn load_trace(path: &str) -> anyhow::Result<Vec<TraceEvent>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    // Try JSONL first (one event per line)
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut events: Vec<TraceEvent> = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let s = line.trim();
        if s.is_empty() {
            continue;
        }
        if let Ok(ev) = serde_json::from_str::<TraceEvent>(s) {
            events.push(ev);
        }
    }
    if !events.is_empty() {
        return Ok(events);
    }
    // Fallback: full JSON array
    let text = std::fs::read_to_string(path)?;
    let vec: Vec<TraceEvent> = serde_json::from_str(&text)?;
    Ok(vec)
}

/// Deterministic clock: maps slots to monotonically increasing timestamps.
#[derive(Debug, Clone, Copy)]
pub struct DeterministicClock {
    pub slot_ms: u64,
}
impl Default for DeterministicClock {
    fn default() -> Self {
        Self { slot_ms: 400 }
    }
}
impl DeterministicClock {
    pub fn ts_for_slot(&self, slot: u64) -> u64 {
        slot.saturating_mul(self.slot_ms)
    }
}

/// Simple schema we understand from Account events when base64-decoded bytes are UTF-8 JSON.
/// This matches the CFM-style pool snapshot (generic CPMM approximation in backtests).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CfmPoolJson {
    pub pool: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_reserve: u128,
    pub quote_reserve: u128,
    pub fee_bps: u32,
}

/// In-memory store built from a trace to serve deterministic responses and derive SimEvents.
#[derive(Debug, Default, Clone)]
pub struct ReplayStore {
    pub slots: Vec<u64>,
    pub logs: Vec<(u64, String)>,
    /// key -> list of updates (in arrival order)
    pub accounts: HashMap<String, Vec<Vec<u8>>>,
}

impl ReplayStore {
    pub fn from_events(events: &[TraceEvent]) -> Self {
        let mut store = ReplayStore {
            slots: Vec::new(),
            logs: Vec::new(),
            accounts: HashMap::new(),
        };
        for ev in events {
            match ev {
                TraceEvent::Slot { slot } => store.slots.push(*slot),
                TraceEvent::Log { slot, msg } => store.logs.push((*slot, msg.clone())),
                TraceEvent::Account { pubkey, data_b64 } => {
                    if let Ok(bytes) = general_purpose::STANDARD.decode(data_b64) {
                        store
                            .accounts
                            .entry(pubkey.clone())
                            .or_default()
                            .push(bytes);
                    }
                }
            }
        }
        store.slots.sort_unstable();
        store.logs.sort_unstable_by_key(|(s, _)| *s);
        store
    }

    /// Build SimEvents in the chosen slot range using a deterministic clock and by projecting logs/accounts we can understand.
    pub fn to_sim_events(&self, cfg: &ReplayConfig) -> Vec<SimEvent> {
        let clock = DeterministicClock {
            slot_ms: cfg.slot_ms.unwrap_or(400),
        };
        let mut out = Vec::new();
        // emit SlotAdvance for every slot in range
        let mut it = SlotIterator::new(cfg.start_slot, cfg.end_slot);
        while let Some(slot) = it.next_slot() {
            out.push(SimEvent {
                ts_ms: clock.ts_for_slot(slot),
                kind: SimEventKind::SlotAdvance { slot },
            });
        }
        // project logs within range
        for (slot, msg) in &self.logs {
            if *slot >= cfg.start_slot && *slot <= cfg.end_slot {
                out.push(SimEvent {
                    ts_ms: clock.ts_for_slot(*slot),
                    kind: SimEventKind::Log(format!("replay: {msg}")),
                });
            }
        }
        // project any account entries that decode to our CfmPoolJson
        for updates in self.accounts.values() {
            for bytes in updates {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    if let Ok(pool) = serde_json::from_str::<CfmPoolJson>(s) {
                        // Emit a NewPool followed by a price/update event with current reserves
                        // Note: no slot associated in generic account events; map to start_slot as baseline
                        let ts = clock.ts_for_slot(cfg.start_slot);
                        out.push(SimEvent {
                            ts_ms: ts,
                            kind: SimEventKind::NewPool {
                                pool: pool.pool.clone(),
                                base_mint: pool.base_mint.clone(),
                                quote_mint: pool.quote_mint.clone(),
                                fee_bps: pool.fee_bps,
                            },
                        });
                        out.push(SimEvent {
                            ts_ms: ts,
                            kind: SimEventKind::CfmPriceUpdate {
                                pool: pool.pool.clone(),
                                base_reserve: pool.base_reserve,
                                quote_reserve: pool.quote_reserve,
                                fee_bps: pool.fee_bps,
                            },
                        });
                    }
                }
            }
        }
        // keep deterministic order by ts then kind string tag (cheap stable sort)
        out.sort_by(|a, b| a.ts_ms.cmp(&b.ts_ms));
        out
    }
}

/// Helper: Build a store and derive events given a ReplayConfig.
pub fn build_events_from_trace(cfg: &ReplayConfig) -> anyhow::Result<(ReplayStore, Vec<SimEvent>)> {
    let events = match cfg.trace_path.as_ref() {
        Some(p) => load_trace(p)?,
        None => Vec::new(),
    };
    let store = ReplayStore::from_events(&events);
    let sim_events = store.to_sim_events(cfg);
    Ok((store, sim_events))
}
