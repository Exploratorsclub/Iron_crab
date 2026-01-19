//! NATS Transport Module – Message Bus for IPC
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §3
//!
//! Topics:
//! - MarketEvents (market-data → consumers)
//! - TradeIntents (strategy → execution-engine)
//! - ExecutionResults (execution-engine → UI/control/analytics)
//! - ControlRequests (control-plane ↔ execution-engine)
//! - PoolCacheUpdates (market-data → execution-engine + arb-strategy via JetStream)

pub mod client;
pub mod jetstream;
pub mod topics;

pub use client::*;
pub use jetstream::*;
pub use topics::*;
